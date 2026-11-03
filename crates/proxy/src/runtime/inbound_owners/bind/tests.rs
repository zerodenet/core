use super::*;
use crate::runtime::Proxy;
use std::{future::Future, task::Poll};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Notify,
    time::{timeout, Duration},
};
use zero_core::{request::Destination, Address};
use zero_platform_tokio::contract::component::ResourceDeclaration;
mod accepted;
mod accepted_direct;
mod addresses;
#[cfg(feature = "vless")]
mod mask_ownership;
#[cfg(feature = "managed-datagram-runtime")]
mod native_peer;
#[cfg(feature = "vless")]
mod vless_continuity;
#[cfg(feature = "vless")]
mod vless_quic;
#[cfg(feature = "vless")]
mod vless_retirement;
#[cfg(all(feature = "vless", unix))]
mod vless_targets;
#[cfg(feature = "vless")]
mod vless_tcp;
#[cfg(feature = "datagram-route-runtime")]
pub(crate) use addresses::test_standard_datagram_listener;
pub(crate) use addresses::test_standard_native_listener;
#[cfg(feature = "datagram-route-runtime")]
mod datagram;
#[cfg(feature = "raw-ip-runtime")]
mod icmp;
mod retirement;

pub(in crate::runtime::inbound_owners) fn proxy() -> Proxy {
    Proxy::new(
        RuntimeConfig::parse(
            r#"{
        "inbounds":[
            {"tag":"first","listen":{"address":"127.0.0.1","port":0},"udp":{"enabled":false},
             "protocol":{"type":"direct","target":"127.0.0.1","port":9}},
            {"tag":"second","listen":{"address":"::1","port":0},"udp":{"enabled":false},
             "protocol":{"type":"direct","target":"127.0.0.1","port":9}}
        ],"route":{"final":{"type":"direct"}}
    }"#,
        )
        .unwrap(),
    )
    .unwrap()
}
pub(in crate::runtime::inbound_owners) fn lease(
    proxy: &Proxy,
    config: &Arc<RuntimeConfig>,
    index: usize,
) -> InboundBindLease {
    let inbound = &config.inbounds[index];
    let prepared = proxy
        .protocols
        .prepare_inbound_bind(inbound.clone(), None)
        .unwrap()
        .expect("Direct TCP uses its actual standard recipe");
    let (shutdown, _) = watch::channel(false);
    proxy
        .inbound_owners
        .reserve_listener(
            &inbound.tag,
            config,
            &proxy.protocols,
            prepared,
            shutdown,
            NonZeroUsize::new(1).unwrap(),
        )
        .unwrap()
}

#[tokio::test]
async fn same_native_listener_survives_cancelled_real_scope_join_and_closes_before_replacement() {
    let proxy = proxy();
    let config = proxy.engine().config();
    let mut lease = lease(&proxy, &config, 0);
    let original = lease.owner();
    let identity = original.identity();
    assert!(proxy.inbound_owners.owner("first").is_none());
    assert!(proxy.inbound_owners.identities().contains(&identity));
    lease.bind_tcp().unwrap();
    let (operation, mut listener, installed) = promote_tcp(lease.finish().unwrap());
    drop(operation); // cold operation never started; the actual task is below
    let owner = installed.owner();
    assert!(Arc::ptr_eq(&original, &owner));
    assert_eq!(owner.identity(), identity);
    let address = listener.local_addr().unwrap();
    let pending = Arc::new(Notify::new());
    let entered = pending.clone();
    owner
        .spawn_driver(async move {
            let (accepted, _) = listener
                .accept_owned()
                .await
                .map_err(std::io::Error::other)?;
            let (_, witness) = accepted
                .spawn_handler_bound(
                    zero_platform_tokio::contract::lifecycle::TaskClass::Auxiliary,
                    move |mut stream, _tasks| async move {
                        let read = stream.read_u8();
                        tokio::pin!(read);
                        let byte = std::future::poll_fn(|cx| match read.as_mut().poll(cx) {
                            Poll::Pending => {
                                entered.notify_one();
                                Poll::Pending
                            }
                            Poll::Ready(result) => Poll::Ready(result),
                        })
                        .await
                        .map_err(|source| {
                            zero_platform_tokio::contract::ChannelError::new(
                                zero_platform_tokio::contract::Stage::Receive,
                                zero_platform_tokio::contract::Reason::Io,
                                zero_platform_tokio::contract::Impact::Session,
                            )
                            .with_source(source)
                        })?;
                        assert_eq!(byte, 71);
                        Ok(())
                    },
                )
                .map_err(std::io::Error::other)?;
            witness.wait().await;
            Ok(())
        })
        .unwrap();
    owner.seal_listener_installation();
    let mut client = TcpStream::connect(address).await.unwrap();
    timeout(Duration::from_secs(2), pending.notified())
        .await
        .unwrap();
    let mut closing = Box::pin(owner.shutdown(Instant::now() + Duration::from_secs(2)));
    std::future::poll_fn(|cx| {
        assert!(closing.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    // The actual native listener closed, but its real registered task is still
    // blocked on accepted I/O. Neither Future cancellation nor native close
    // alone can fabricate a TaskExit or complete the owning instance.
    let replacement = TcpListener::bind(address).await.unwrap();
    assert!(owner.terminal().is_none());
    drop(closing);
    client.write_all(&[71]).await.unwrap();
    let receipt = timeout(
        Duration::from_secs(2),
        proxy
            .inbound_owners
            .confirm_shutdown("first", Instant::now() + Duration::from_secs(2)),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    assert_eq!(receipt.terminal().joined_tasks(), 2);
    assert_eq!(receipt.terminal().closed_io_receipts(), 2);
    assert!(receipt.terminal().failure().is_none());
    proxy
        .inbound_owners
        .release_closed_reported(receipt, |_| Ok(()))
        .unwrap();
    assert!(proxy.inbound_owners.owner("first").is_none());
    assert_eq!(replacement.local_addr().unwrap(), address);
}

#[tokio::test]
async fn dropped_partial_reservations_hold_native_fds_until_explicit_batch_cleanup() {
    let proxy = proxy();
    let config = proxy.engine().config();
    let mut first = lease(&proxy, &config, 0);
    let mut second = lease(&proxy, &config, 1);
    first.bind_tcp().unwrap();
    second.bind_tcp().unwrap();
    let first = first.finish().unwrap();
    let second = second.finish().unwrap();
    let first_address = first.local_addr().unwrap();
    let second_address = second.local_addr().unwrap();
    // A later bind/preparation failure or canceled reserved-map owner loses
    // only these views, never the actual native controls in Registry.
    drop((first, second));
    assert!(TcpListener::bind(first_address).await.is_err());
    assert!(TcpListener::bind(second_address).await.is_err());
    assert_eq!(proxy.inbound_owners.identities().len(), 2);
    let mut changed = (*config).clone();
    changed.runtime.event_log_capacity += 1;
    let changed = Arc::new(changed);
    let prepared = proxy
        .protocols
        .prepare_inbound_bind(changed.inbounds[0].clone(), None)
        .unwrap()
        .unwrap();
    let (stop, _) = watch::channel(false);
    assert!(proxy
        .inbound_owners
        .reserve_listener(
            "first",
            &changed,
            &proxy.protocols,
            prepared,
            stop,
            NonZeroUsize::new(1).unwrap()
        )
        .is_err());
    let mut published = Vec::new();
    proxy
        .inbound_owners
        .retire_bind_reservations_reported(
            Instant::now() + Duration::from_secs(2),
            |tag, terminal| {
                assert_eq!(terminal.joined_tasks(), 0);
                assert_eq!(terminal.closed_io_receipts(), 1);
                assert!(terminal.failure().is_none());
                published.push((tag.to_owned(), terminal.identity()));
                Ok(())
            },
        )
        .await
        .unwrap();
    assert_eq!(published.len(), 2);
    let _first_closed = TcpListener::bind(first_address).await.unwrap();
    let _second_closed = TcpListener::bind(second_address).await.unwrap();
    assert!(proxy.inbound_owners.identities().is_empty());
    let mut next = lease(&proxy, &changed, 0);
    next.bind_tcp().unwrap();
    drop(next);
    proxy
        .inbound_owners
        .shutdown_all_reported(Instant::now() + Duration::from_secs(2), |_, _| Ok(()))
        .await
        .unwrap();
}

// Only projects the real bound view; it never invents a protocol operation.
pub(in crate::runtime::inbound_owners) fn promote_tcp(
    lease: LeasedInbound,
) -> (
    crate::component::ComponentRecipe,
    zero_platform_tokio::ControlledTcpListener,
    InboundInstallContext,
) {
    let (definition, view, installed) = lease.promote_definition().unwrap();
    let NativeListenerView::Tcp(listener) = view else {
        panic!("actual TCP declaration");
    };
    (definition, listener, installed)
}

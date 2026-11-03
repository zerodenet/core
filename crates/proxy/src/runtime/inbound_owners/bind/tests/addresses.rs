//! Actual standard factory native grants and same-entry Scope receipts.
use super::*;
pub(crate) async fn test_standard_native_listener(inbound: zero_config::InboundConfig) {
    use std::{num::NonZeroUsize, sync::Arc};
    use zero_platform_tokio::contract::lifecycle::TaskClass;
    let config = zero_config::RuntimeConfig::parse(
        &serde_json::json!({
            "inbounds":[inbound], "route":{"final":{"type":"direct"}}
        })
        .to_string(),
    )
    .unwrap();
    let proxy = crate::runtime::Proxy::new(config).unwrap();
    let config = proxy.engine().config();
    let prepared = proxy
        .protocols
        .prepare_inbound_bind(config.inbounds[0].clone(), None)
        .unwrap()
        .unwrap();
    let (stop, _) = tokio::sync::watch::channel(false);
    let mut lease = proxy
        .inbound_owners
        .reserve_listener(
            "tcp-ipv6",
            &config,
            &proxy.protocols,
            prepared,
            stop,
            NonZeroUsize::new(1).unwrap(),
        )
        .unwrap();
    lease.resolve_native().await.unwrap();
    lease.bind_native().unwrap();
    let (definition, view, installed) = lease.finish().unwrap().promote_definition().unwrap();
    drop(definition); // This fixture tests the original native grant, not a wire handshake.
    let crate::runtime::inbound_owners::NativeListenerView::Tcp(mut listener) = view else {
        panic!("actual TCP resource declaration");
    };
    let owner = installed.owner();
    let mut address = listener.local_addr().unwrap();
    assert!(address.is_ipv6());
    if address.ip().is_unspecified() {
        address.set_ip(std::net::Ipv6Addr::LOCALHOST.into());
    }
    owner
        .spawn_driver(async move {
            let (accepted, _) = listener
                .accept_owned()
                .await
                .map_err(std::io::Error::other)?;
            let (_, witness) = accepted
                .spawn_handler_bound(TaskClass::Auxiliary, |mut io, _| async move {
                    let mut bytes = [0; 4];
                    io.read_exact(&mut bytes).await.map_err(|source| {
                        zero_platform_tokio::contract::ChannelError::new(
                            zero_platform_tokio::contract::Stage::Receive,
                            zero_platform_tokio::contract::Reason::Io,
                            zero_platform_tokio::contract::Impact::Session,
                        )
                        .with_source(source)
                    })?;
                    assert_eq!(&bytes, b"ipv6");
                    Ok(())
                })
                .map_err(std::io::Error::other)?;
            witness.wait().await;
            Ok(())
        })
        .unwrap();
    owner.seal_listener_installation();
    let (observed, _) = tokio::join!(owner.observe_driver(), async {
        let mut client = TcpStream::connect(address).await.unwrap();
        client.write_all(b"ipv6").await.unwrap();
    });
    observed.unwrap();
    let receipt = proxy
        .inbound_owners
        .confirm_shutdown(
            "tcp-ipv6",
            tokio::time::Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap()
        .unwrap();
    assert!(Arc::ptr_eq(&owner, &installed.owner()));
    assert!(receipt.terminal().failure().is_none());
    assert_eq!(receipt.terminal().joined_tasks(), 2);
    assert_eq!(receipt.terminal().closed_io_receipts(), 2);
}

#[cfg(feature = "datagram-route-runtime")]
pub(crate) async fn test_standard_datagram_listener(inbound: zero_config::InboundConfig) {
    let tag = inbound.tag.clone();
    let config = RuntimeConfig::parse(
        &serde_json::json!({
            "inbounds":[inbound], "route":{"final":{"type":"direct"}}
        })
        .to_string(),
    )
    .unwrap();
    let proxy = Proxy::new(config).unwrap();
    let config = proxy.engine().config();
    let prepared = proxy
        .protocols
        .prepare_inbound_bind(config.inbounds[0].clone(), None)
        .unwrap()
        .expect("standard datagram factory");
    let (stop, _) = tokio::sync::watch::channel(false);
    let mut lease = proxy
        .inbound_owners
        .reserve_listener(
            &tag,
            &config,
            &proxy.protocols,
            prepared,
            stop,
            NonZeroUsize::new(1).unwrap(),
        )
        .unwrap();
    lease.resolve_native().await.unwrap();
    lease.bind_native().unwrap();
    let (definition, view, installed) = lease.finish().unwrap().promote_definition().unwrap();
    assert!(definition
        .spec()
        .roles
        .contains(&zero_platform_tokio::contract::component::RoleKind::Inbound));
    let NativeListenerView::Datagram(mut socket) = view else {
        panic!("original UDP grant")
    };
    let owner = installed.owner();
    let mut address = socket.rx.local_addr().unwrap();
    assert!(address.is_ipv6());
    address.set_ip(std::net::Ipv6Addr::LOCALHOST.into());
    owner
        .spawn_driver(async move {
            let mut bytes = [0; 64];
            let (n, peer) = socket.rx.recv_from_addr(&mut bytes).await?;
            assert_eq!(&bytes[..n], b"packet");
            socket.tx.send_to_addr(&bytes[..n], peer).await?;
            Ok(())
        })
        .unwrap();
    owner.seal_listener_installation();
    let (observed, ()) = tokio::join!(owner.observe_driver(), async {
        let client = tokio::net::UdpSocket::bind((std::net::Ipv6Addr::LOCALHOST, 0))
            .await
            .unwrap();
        client.send_to(b"packet", address).await.unwrap();
        let mut bytes = [0; 64];
        let n = timeout(Duration::from_secs(3), client.recv(&mut bytes))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&bytes[..n], b"packet");
    });
    observed.unwrap();
    let receipt = proxy
        .inbound_owners
        .confirm_shutdown(&tag, Instant::now() + Duration::from_secs(3))
        .await
        .unwrap()
        .unwrap();
    assert!(receipt.terminal().failure().is_none());
    assert_eq!(receipt.terminal().joined_tasks(), 1);
    assert_eq!(receipt.terminal().closed_io_receipts(), 1);
    proxy
        .inbound_owners
        .release_closed(receipt, |_| Ok(()))
        .unwrap();
}

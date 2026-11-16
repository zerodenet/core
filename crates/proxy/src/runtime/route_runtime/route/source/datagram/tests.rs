//! Real Source parent, production dispatcher Owner, original views and native receipts.
use super::*;
use crate::runtime::{pipe::UdpPipeInput, udp_dispatch::LiveDirectSource};
use tokio::{
    net::UdpSocket,
    time::{timeout, Duration, Instant},
};
use zero_core::{Address, ProtocolType};
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}
fn input<'a>(peer: std::net::SocketAddr, payload: &'a [u8]) -> UdpPipeInput<'a> {
    UdpPipeInput {
        target: Address::Ipv4([127, 0, 0, 1]),
        port: peer.port(),
        payload,
        auth: None,
        source_addr: None,
        protocol: ProtocolType::new("direct"),
        peer_identity: None,
        route_target: None,
        sniffed_original_target: None,
        sniffed_host_source: None,
        skip_fake_ip_restore: false,
        client_session_id: None,
        transparent_target: false,
        transparent_original_target: None,
        transparent_host_source: None,
        domain_memory: None,
    }
}
#[tokio::test]
async fn installed_datagram_dispatch_shares_the_original_bound_provider_across_targets_and_retires_actual_views(
) {
    timeout(Duration::from_secs(15), async {
        let live = LiveDirectSource::new().await;
        let returns = Arc::new(crate::component::datagram_retirement::DatagramReturns::default());
        let state = Arc::new(AsyncMutex::new(owner::State::default()));
        let stop = live.context.binding().cancellation();
        let runtime = live
            .runtime
            .clone()
            .with_datagram_returns(returns.clone())
            .with_inline_direct_driver(live.driver_slot.clone(), stop.clone());
        // The same production cleanup Owner is installed before dispatcher
        // preparation/start. No test-created success or Drop substitutes custody.
        live.context
            .claim(deadline(), |_| {
                Ok((
                    erase_lifecycle(owner::Owner {
                        state: state.clone(),
                        cancel: stop,
                        returns,
                        retired: live.proxy.retired_datagrams.clone(),
                        peer_retirement: None,
                    }),
                    (),
                ))
            })
            .await
            .unwrap();
        let caller = live.context.binding().join_witness().unwrap();
        let totals = live
            .proxy
            .protocols
            .runtime_host()
            .original_budgets()
            .carrier
            .clone();
        let first = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let second = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut original = state.lock().await;
        original.dispatch = Some(runtime.new_dispatch("source").await.unwrap());
        let dispatch = original.dispatch.as_mut().unwrap();
        let mut previous: Option<
            zero_platform_tokio::controlled_datagram::provider::NativeDatagramOrigin,
        > = None;
        let mut retained = Vec::new();
        for (target, payload) in [(&first, b"".as_slice()), (&second, b"second".as_slice())] {
            let session = dispatch
                .dispatch(input(target.local_addr().unwrap(), payload))
                .await
                .unwrap();
            assert_ne!(session, 0);
            let mut bytes = [0; 64];
            let (size, source) = target.recv_from(&mut bytes).await.unwrap();
            assert_eq!(&bytes[..size], payload);
            target.send_to(payload, source).await.unwrap();
            let (size, source) = dispatch
                .poll_sockets()
                .0
                .recv_from_addr(&mut bytes)
                .await
                .unwrap();
            assert_eq!(&bytes[..size], payload);
            assert!(source.is_current());
            assert_eq!(source.session_id, Some(session));
            let originals = dispatch.direct_original_views_for_test();
            assert_eq!(
                originals.len(),
                1,
                "targets share the actual policy/family original"
            );
            let (native, view) = &originals[0];
            assert!(native.driver().same_registration(&caller));
            assert!(native.closed().is_none() && !view.is_returned());
            if let Some(old) = &previous {
                assert!(old.same_original(native));
            }
            previous = Some(native.clone());
            retained.push(source);
        }
        let old = dispatch.direct_original_views_for_test();
        let first_session = retained[0].session_id.unwrap();
        live.runtime
            .services()
            .upstream()
            .egress_control()
            .invalidate_network();
        assert!(retained.iter().all(|source| !source.is_current()));
        let same = dispatch
            .dispatch(input(first.local_addr().unwrap(), b"after-network"))
            .await
            .unwrap();
        assert_eq!(
            same, first_session,
            "network handover preserves the actual business flow"
        );
        let mut bytes = [0; 64];
        let (size, new_source) = first.recv_from(&mut bytes).await.unwrap();
        assert_eq!(&bytes[..size], b"after-network");
        first.send_to(b"after-network", new_source).await.unwrap();
        let (size, response) = dispatch
            .poll_sockets()
            .0
            .recv_from_addr(&mut bytes)
            .await
            .unwrap();
        assert_eq!(&bytes[..size], b"after-network");
        assert_eq!(response.session_id, Some(first_session));
        for (native, view) in old {
            assert!(native
                .closed()
                .expect("old actual native original closed before new epoch")
                .matches(native.owner()));
            assert!(view.is_returned());
        }
        retained.push(response);
        let originals = dispatch.direct_original_views_for_test();
        assert_eq!(originals.len(), 1);
        assert!(totals.snapshot().charged > 0);
        drop(original);
        // This invokes host request_stop/full observer, consumes this same
        // original peer join, then drives the installed production Owner.
        live.close().await;
        for (native, view) in originals {
            let receipt = native.closed().expect("original provider close");
            assert!(receipt.matches(native.owner()));
            assert!(view.is_returned());
        }
        assert!(caller.is_joined());
        assert!(retained.iter().all(|source| !source.is_current()));
        assert_eq!(totals.snapshot().charged, 0);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn actual_standard_direct_source_charges_host_originals_and_consumes_them_on_shutdown() {
    timeout(Duration::from_secs(15),async {
        let target=UdpSocket::bind("127.0.0.1:0").await.unwrap();let address=target.local_addr().unwrap();
        let config=zero_config::RuntimeConfig::parse(&serde_json::json!({
            "inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"direct","target":"127.0.0.1","port":address.port()}}],
            "route":{"final":{"type":"direct"}}
        }).to_string()).unwrap();
        let proxy=crate::Proxy::new(config).unwrap();let registry=proxy.system_tun_source_registry();
        let totals=proxy.protocols.runtime_host().original_budgets().carrier.clone();
        let running=proxy.spawn();
        let owner=loop {if let Some(owner)=registry.owner("entry"){break owner;}tokio::task::yield_now().await;};
        owner.wait_ready().await.unwrap();let entry=*owner.original_listener_address.get().unwrap();
        let client=UdpSocket::bind("127.0.0.1:0").await.unwrap();client.send_to(b"",entry).await.unwrap();
        let mut bytes=[0;64];let (size,source)=target.recv_from(&mut bytes).await.unwrap();assert_eq!(size,0);
        assert!(totals.snapshot().charged>0,"ordinary Source uses the actual Host native provider");
        target.send_to(b"reply",source).await.unwrap();let (size,from)=client.recv_from(&mut bytes).await.unwrap();assert_eq!(&bytes[..size],b"reply");assert_eq!(from,entry);
        running.shutdown().await.unwrap();
        let terminal=owner.terminal_for_test().unwrap();assert_eq!(terminal.identity(),owner.identity());
        assert!(terminal.failure().is_none() && terminal.task_failures().is_empty() && terminal.native_bound_failures().is_empty());
        assert!(terminal.closed_io_receipts()>=2 && terminal.joined_tasks()>0);
        assert_eq!(totals.snapshot().charged,0);
    }).await.unwrap();
}

#[cfg(feature = "socks5")]
#[path = "tests/socks.rs"]
mod socks;

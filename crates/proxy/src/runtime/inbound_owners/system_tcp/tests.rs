//! The public API exercises a standard Source and Node's actual native provider.
use super::*;
use crate::{adapters::socks5::relay_tests::fixtures, Proxy, ProxyHandle};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};
fn config(peer: SocketAddr) -> zero_config::RuntimeConfig {
    zero_config::RuntimeConfig::parse(&serde_json::json!({
        "outbounds":[{"tag":"node","protocol":{"type":"socks5","server":"127.0.0.1","port":peer.port()}}],
        "route":{"final":{"type":"route","outbound":"node"}}
    }).to_string()).unwrap()
}
fn handle(proxy: &Proxy) -> ProxyHandle {
    ProxyHandle::new(
        zero_engine::EngineHandle::new(proxy.engine().clone()),
        proxy.clone(),
    )
}
async fn exchange(peer: &TcpListener, entry: SocketAddr, target_ip: std::net::IpAddr) {
    let mut front = TcpStream::connect(entry).await.unwrap();
    let (mut back, _) = peer.accept().await.unwrap();
    let target = fixtures::request(&mut back).await;
    let ip = match target_ip {
        std::net::IpAddr::V4(ip) => zero_core::Address::Ipv4(ip.octets()),
        std::net::IpAddr::V6(ip) => zero_core::Address::Ipv6(ip.octets()),
    };
    assert_eq!(
        target,
        (ip, entry.port()),
        "historical listener-local destination"
    );
    fixtures::accept(&mut back).await;
    front.write_all(b"system-upload").await.unwrap();
    let mut upload = [0; 13];
    back.read_exact(&mut upload).await.unwrap();
    assert_eq!(&upload, b"system-upload");
    front.shutdown().await.unwrap();
    assert_eq!(
        back.read_u8().await.unwrap_err().kind(),
        std::io::ErrorKind::UnexpectedEof
    );
    back.write_all(b"system-tail").await.unwrap();
    back.shutdown().await.unwrap();
    let mut tail = Vec::new();
    front.read_to_end(&mut tail).await.unwrap();
    assert_eq!(tail, b"system-tail");
}
async fn idle(source: &InboundOwner) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        match source.pause_acquisition_if_idle(deadline).await {
            Ok(pause) => {
                drop(pause);
                return;
            }
            Err(EngineError::Io(source)) if source.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "original Source must acknowledge real idle"
                );
                tokio::task::yield_now().await;
            }
            Err(source) => panic!("original Source idle failed: {source}"),
        }
    }
}
#[tokio::test]
async fn public_system_source_preserves_local_targets_halfclose_node_provider_and_original_retirement(
) {
    timeout(Duration::from_secs(25), async {
        for listen in ["127.0.0.1:0", "0.0.0.0:0"] {
            // Keep this real peer reserved until its actual accept operation.
            let peer = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let config = config(peer.local_addr().unwrap());
            let proxy = Proxy::new(config.clone()).unwrap();
            let running = proxy.spawn();
            handle(&proxy)
                .apply_runtime_config_and_wait(config, Duration::from_secs(5))
                .await
                .unwrap();
            let listen: SocketAddr = listen.parse().unwrap();
            proxy
                .start_system_inbound(listen, "redirect")
                .await
                .unwrap();
            let source = proxy.inbound_owners.owner(&key("redirect")).unwrap();
            let bound = *source.original_listener_address.get().unwrap();
            let entry = SocketAddr::new("127.0.0.1".parse().unwrap(), bound.port());
            let services = proxy.tcp_runtime_services();
            let plan = services.components.as_ref().unwrap();
            let native = plan
                .configured("node")
                .unwrap()
                .original_carrier_owner(0)
                .unwrap();
            // Capture a real origin while I/O is outstanding, before return.
            let mut front = TcpStream::connect(entry).await.unwrap();
            let (mut back, _) = peer.accept().await.unwrap();
            assert_eq!(
                fixtures::request(&mut back).await,
                (
                    match listen.ip() {
                        std::net::IpAddr::V4(ip) => zero_core::Address::Ipv4(ip.octets()),
                        _ => unreachable!(),
                    },
                    entry.port()
                )
            );
            fixtures::accept(&mut back).await;
            front.write_all(b"live").await.unwrap();
            let mut live = [0; 4];
            back.read_exact(&mut live).await.unwrap();
            assert_eq!(&live, b"live");
            let observation = native.observe().unwrap();
            assert_eq!(observation.retained(), 1);
            let original = observation.slots()[0].original().clone();
            assert!(original.closed().is_none());
            front.shutdown().await.unwrap();
            assert!(back.read_u8().await.is_err());
            back.shutdown().await.unwrap();
            let mut end = Vec::new();
            front.read_to_end(&mut end).await.unwrap();
            assert!(end.is_empty());
            drop((front, back));
            // The API did not drop its only stop sender on return; a second
            // exchange uses the same original accepted Source and actual Node.
            exchange(&peer, entry, listen.ip()).await;
            running.shutdown().await.unwrap();
            let closed = original.closed().expect("original Node close receipt");
            assert!(original.matches_closed(&closed));
            let terminal = source
                .terminal_for_test()
                .expect("original Source terminal");
            assert!(
                terminal.failure().is_none()
                    && terminal.task_failures().is_empty()
                    && terminal.native_bound_failures().is_empty()
            );
            assert!(terminal.closed_io_receipts() > 0 && terminal.joined_tasks() > 0);
            assert_eq!(native.observe().unwrap().retained(), 0);
            assert_eq!(
                proxy
                    .protocols
                    .runtime_host()
                    .original_budgets()
                    .carrier
                    .snapshot()
                    .charged,
                0
            );
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn system_api_cold_input_survives_run_start_and_restart_only_after_original_source_retirement(
) {
    timeout(Duration::from_secs(25), async {
        let peer = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = config(peer.local_addr().unwrap());
        let proxy = Proxy::new(config.clone()).unwrap();
        proxy
            .start_system_inbound("127.0.0.1:0".parse().unwrap(), "redirect")
            .await
            .unwrap();
        let cold = proxy.inbound_owners.owner(&key("redirect")).unwrap();
        let entry = *cold.original_listener_address.get().unwrap();
        let running = proxy.spawn();
        handle(&proxy)
            .apply_runtime_config_and_wait(config.clone(), Duration::from_secs(5))
            .await
            .unwrap();
        let active = proxy.inbound_owners.owner(&key("redirect")).unwrap();
        assert_ne!(cold.identity(), active.identity());
        let old = cold
            .terminal_for_test()
            .expect("cold original actually retired");
        assert!(old.closed_io_receipts() > 0 && old.joined_tasks() > 0);
        assert_eq!(*active.original_listener_address.get().unwrap(), entry);
        exchange(&peer, entry, entry.ip()).await;
        running.shutdown().await.unwrap();
        let first = active.terminal_for_test().unwrap();
        assert!(first.closed_io_receipts() > 0 && first.failure().is_none());
        let running = proxy.spawn();
        handle(&proxy)
            .apply_runtime_config_and_wait(config, Duration::from_secs(5))
            .await
            .unwrap();
        let replacement = proxy.inbound_owners.owner(&key("redirect")).unwrap();
        assert_ne!(replacement.identity(), active.identity());
        assert_eq!(*replacement.original_listener_address.get().unwrap(), entry);
        exchange(&peer, entry, entry.ip()).await;
        running.shutdown().await.unwrap();
        assert!(
            replacement
                .terminal_for_test()
                .unwrap()
                .closed_io_receipts()
                > 0
        );
    })
    .await
    .unwrap();
}

#[path = "tests/reload.rs"]
mod reload;

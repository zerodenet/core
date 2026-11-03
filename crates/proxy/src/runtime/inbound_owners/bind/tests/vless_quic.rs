//! Actual standard factory, TCP/Datagram dispatch and original no-overlap receipts.
use super::*;
use zero_platform_tokio::TcpRelayStream;
const USER: &str = "11111111-2222-3333-4444-555555555555";
fn request(command: u8, port: u16) -> Vec<u8> {
    let mut b = vec![0];
    b.extend(vless::parse_uuid(USER).unwrap());
    b.extend([0, command]);
    b.extend(port.to_be_bytes());
    b.extend([1, 127, 0, 0, 1]);
    b
}
#[path = "vless_quic/client.rs"]
mod client;
use client::{masks, open, Client};
#[path = "vless_quic/masquerade.rs"]
mod masquerade;
#[tokio::test]
async fn vless_quic_hysteria_file_originals_close_before_restart() {
    timeout(Duration::from_secs(45), exercise("hysteria-file"))
        .await
        .unwrap();
}
#[tokio::test]
async fn vless_quic_hysteria_proxy_originals_close_before_restart() {
    timeout(Duration::from_secs(45), exercise("hysteria-proxy"))
        .await
        .unwrap();
}
#[tokio::test]
async fn vless_quic_noise_originals_close_before_restart() {
    timeout(Duration::from_secs(45), exercise("quic-noise"))
        .await
        .unwrap();
}
#[tokio::test]
async fn vless_mkcp_xdns_originals_close_before_restart() {
    timeout(Duration::from_secs(45), exercise("mkcp-xdns"))
        .await
        .unwrap();
}
#[tokio::test]
#[ignore = "requires raw ICMP receive permission for the actual XICMP server"]
async fn vless_mkcp_xicmp_originals_close_before_restart() {
    timeout(Duration::from_secs(45), exercise("mkcp-xicmp"))
        .await
        .unwrap();
}
#[tokio::test]
async fn vless_quic_raw_originals_close_before_restart() {
    timeout(Duration::from_secs(45), exercise("quic"))
        .await
        .unwrap();
}
#[tokio::test]
async fn vless_quic_h3_stream_up_originals_close_before_restart() {
    timeout(Duration::from_secs(45), exercise("h3-stream-up"))
        .await
        .unwrap();
}
#[tokio::test]
async fn vless_quic_h3_packet_up_originals_close_before_restart() {
    timeout(Duration::from_secs(45), exercise("h3-packet-up"))
        .await
        .unwrap();
}
#[tokio::test]
async fn vless_quic_h3_stream_one_originals_close_before_restart() {
    timeout(Duration::from_secs(45), exercise("h3-stream-one"))
        .await
        .unwrap();
}
#[tokio::test]
async fn vless_quic_hysteria_originals_close_before_restart() {
    timeout(Duration::from_secs(45), exercise("hysteria"))
        .await
        .unwrap();
}
#[tokio::test]
async fn vless_quic_hysteria_proxy_cancelled_response_originals_close_before_restart() {
    timeout(Duration::from_secs(45), exercise("hysteria-proxy-cancel"))
        .await
        .unwrap();
}
#[tokio::test]
async fn vless_quic_hysteria_proxy_https_originals_close_before_restart() {
    timeout(Duration::from_secs(45), exercise("hysteria-proxy-https"))
        .await
        .unwrap();
}
#[tokio::test]
async fn vless_quic_hysteria_proxy_h2_originals_close_before_restart() {
    timeout(Duration::from_secs(45), exercise("hysteria-proxy-h2"))
        .await
        .unwrap();
}
async fn exercise(carrier: &str) {
    let files = tempfile::tempdir().unwrap();
    let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert = files.path().join("ca.pem");
    let key = files.path().join("key.pem");
    std::fs::write(&cert, certificate.cert.pem()).unwrap();
    std::fs::write(&key, certificate.signing_key.serialize_pem()).unwrap();

    let echo = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let udp_echo = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut protocol = serde_json::json!({"type":"vless","users":[{"id":USER}],"quic":{"cert_path":cert,"key_path":key}});
    if carrier.starts_with("h3-") {
        protocol["split_http"] = serde_json::json!({"path":"/scope/","mode":"auto"});
    }
    if carrier.starts_with("hysteria") {
        protocol["hysteria"] = serde_json::json!({"auth":"original"});
    }
    let appearance =
        masquerade::Fixture::new(carrier, files.path(), &mut protocol, &certificate).await;
    let (_, wire_masks) = masks(carrier);
    if !wire_masks.is_empty() {
        protocol["final_mask"] = masks(carrier).0;
    }
    let mkcp = carrier.starts_with("mkcp");
    let mtu = if carrier.ends_with("xdns") { 100 } else { 1200 };
    if mkcp {
        protocol.as_object_mut().unwrap().remove("quic");
        protocol["mkcp"] = serde_json::json!({"mtu":mtu});
    }
    let config=RuntimeConfig::parse(&serde_json::json!({"inbounds":[{"tag":"first","listen":{"address":"127.0.0.1","port":0},"protocol":protocol}],"route":{"final":{"type":"direct"}}}).to_string()).unwrap();
    let proxy = Proxy::new(config).unwrap();
    let config = proxy.engine().config();
    proxy
        .protocols
        .start_components(config.clone(), Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    proxy
        .protocols
        .commit_components(&config, || Ok(()))
        .unwrap();
    let factory = crate::runtime::route_runtime::InboundListenerRuntimeFactory::new(
        crate::runtime::route_runtime::SharedIngressRuntimeServices::new(
            proxy.tcp_runtime_services(),
        ),
        proxy.inbound_owners.clone(),
    );
    for generation in 0..2 {
        eprintln!("{carrier} generation {generation}: original start");
        let prepared = proxy
            .protocols
            .prepare_inbound_bind(config.inbounds[0].clone(), None)
            .unwrap()
            .expect("actual standard registration");
        let (stop, _) = watch::channel(false);
        let mut lease = proxy
            .inbound_owners
            .reserve_listener(
                "first",
                &config,
                &proxy.protocols,
                prepared,
                stop.clone(),
                NonZeroUsize::new(1).unwrap(),
            )
            .unwrap();
        lease.bind_native().unwrap();
        let (definition, view, installed) = lease.finish().unwrap().promote_definition().unwrap();
        let owner = installed.owner();
        let recipe = definition;
        assert!(matches!(
            recipe.spec().resources.first(),
            Some(ResourceDeclaration::MetadataDatagram { .. })
        ));
        let upstream = factory
            .for_inbound("first".into())
            .route_factory()
            .for_connection(None)
            .upstream_services();
        let (source, peers) = installed
            .build_inbound(recipe, view, Some(upstream))
            .unwrap();
        let local = peers.as_ref().unwrap().carrier.clone().unwrap();
        let runtime = factory
            .for_inbound("first".into())
            .with_inbound_instance(owner.identity())
            .with_readiness(owner.readiness())
            .with_datagram_source(peers);
        owner
            .spawn_driver(crate::runtime::inbound_operation::source::run(
                source,
                runtime,
                stop.subscribe(),
                NonZeroUsize::new(4096).unwrap(),
                "vless",
            ))
            .unwrap();
        owner.seal_listener_installation();
        let business = async {
            owner.wait_ready().await.unwrap();
            let address = *local.get().expect("original bound endpoint");
            let client = client::new(carrier, wire_masks.clone(), mtu, &cert);
            appearance.exercise(&client, address).await;
            assert_eq!(
                proxy.engine().completed_sessions().len(),
                2 * generation,
                "appearance has no business identity"
            );
            let mut held = Vec::new();
            if carrier == "quic" {
                for _ in 0..9 {
                    if let Client::Quic(endpoint) = &client {
                        held.push(
                            endpoint
                                .connect(address, "localhost")
                                .unwrap()
                                .await
                                .unwrap(),
                        );
                    }
                }
            }
            let (connection, mut stream) = open(&client, address, carrier).await;
            eprintln!("{carrier} generation {generation}: carrier established");
            assert!(
                proxy.engine().active_sessions().is_empty(),
                "native/carrier authentication has no business identity"
            );
            let mut header = request(1, echo.local_addr().unwrap().port());
            header.extend(b"real");
            stream.write_all(&header).await.unwrap();
            stream.flush().await.unwrap();
            let mut target = echo.accept().await.unwrap().0;
            let mut bytes = [0; 4];
            target.read_exact(&mut bytes).await.unwrap();
            assert_eq!(&bytes, b"real");
            target.write_all(b"back").await.unwrap();
            let mut reply = [0; 6];
            stream.read_exact(&mut reply).await.unwrap();
            assert_eq!(&reply, b"\0\0back");
            eprintln!("{carrier} generation {generation}: TCP complete");
            let (udp_connection, mut datagram) = open(&client, address, carrier).await;
            let mut header = request(2, udp_echo.local_addr().unwrap().port());
            header.extend(b"\0\x03udp");
            datagram.write_all(&header).await.unwrap();
            datagram.flush().await.unwrap();
            let mut bytes = [0; 64];
            let (size, sender) = udp_echo.recv_from(&mut bytes).await.unwrap();
            assert_eq!(&bytes[..size], b"udp");
            udp_echo.send_to(b"datagram", sender).await.unwrap();
            let mut reply = [0; 12];
            datagram.read_exact(&mut reply).await.unwrap();
            assert_eq!(&reply, b"\0\0\0\x08datagram");
            let mut byte = [0];
            let mut suspended = Box::pin(target.read(&mut byte));
            std::future::poll_fn(|cx| {
                assert!(suspended.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            eprintln!("{carrier} generation {generation}: UDP complete");
            assert!(owner.terminal().is_none());
            stop.send_replace(true);
            assert_eq!(suspended.await.unwrap(), 0, "real egress close");
            drop(stream);
            drop(datagram);
            if let Some(connection) = &connection {
                connection.close(0u32.into(), b"retired");
            }
            if let Some(connection) = &udp_connection {
                connection.close(0u32.into(), b"retired");
            }
            drop(connection);
            drop(udp_connection);
            for pending in held {
                pending.close(0u32.into(), b"retired");
            }
            if let Client::Quic(client) = client {
                client.close(0u32.into(), b"retired");
                client.wait_idle().await;
            }
        };
        let (observed, ()) = tokio::join!(owner.observe_driver(), business);
        observed.unwrap_or_else(|e| panic!("{carrier} generation {generation}: {e:?}"));
        let receipt = proxy
            .inbound_owners
            .confirm_shutdown("first", Instant::now() + Duration::from_secs(3))
            .await
            .unwrap()
            .unwrap();
        assert!(
            receipt.terminal().failure().is_none(),
            "{:?}",
            receipt.terminal().failure()
        );
        assert!(
            receipt.terminal().joined_tasks() >= 6,
            "source, endpoint, two connections and two real callers"
        );
        assert!(
            receipt.terminal().closed_io_receipts()
                >= if carrier == "hysteria-file" {
                    7
                } else if carrier.starts_with("hysteria-proxy") {
                    4
                } else {
                    3
                },
            "metadata and actual TCP/UDP originals"
        );
        assert!(proxy.engine().active_sessions().is_empty());
        assert_eq!(
            proxy.engine().completed_sessions().len(),
            2 * (generation + 1)
        );
        let stats = proxy.engine().stats_snapshot();
        assert!(
            stats.bytes_up >= 7 * (generation as u64 + 1)
                && stats.bytes_down >= 12 * (generation as u64 + 1),
            "actual business traffic is accounted"
        );
        assert!(
            proxy.inbound_owners.open_bind_admission().is_err(),
            "original receipt not yet released"
        );
        proxy
            .inbound_owners
            .release_closed(receipt, |_| Ok(()))
            .unwrap();
    }
}

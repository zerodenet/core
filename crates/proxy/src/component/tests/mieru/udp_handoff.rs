//! Exercise the existing ordinary UDP path after its claimer is removed.
use super::*;
use crate::runtime::{pipe::UdpPipeInput, udp_ingress::test_source};
use zero_core::{Address, ProtocolType};

#[cfg(feature = "socks5")]
#[tokio::test]
async fn standard_outbound_shared_relay_selector_uses_one_native_pool_with_a_tcp_only_business_suffix(
) {
    tokio::time::timeout(Duration::from_secs(20), async {
        let server = Server::new(false).await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let first_port = listener.local_addr().unwrap().port();
        let last = server.address;
        let retiring = zero_platform_tokio::contract::CancellationToken::new();
        let host_retiring = retiring.clone();
        let peer = tokio::spawn(async move {
            let (mut incoming, _) = listener.accept().await.unwrap();
            let target = crate::adapters::socks5::relay_tests::fixtures::request(&mut incoming).await;
            assert_eq!(target, (Address::Domain("127.0.0.1".into()), last.port()));
            let mut outgoing = tokio::net::TcpStream::connect(last).await.unwrap();
            crate::adapters::socks5::relay_tests::fixtures::accept(&mut incoming).await;
            if let Err(error) = tokio::io::copy_bidirectional(&mut incoming, &mut outgoing).await {
                assert!(host_retiring.is_cancelled() && error.kind() == std::io::ErrorKind::ConnectionReset, "{error}");
            }
        });
        let config = zero_config::RuntimeConfig::parse(&serde_json::json!({
            "outbounds":[
                {"tag":"first","protocol":{"type":"socks5","server":"127.0.0.1","port":first_port}},
                {"tag":"node","protocol":{"type":"mieru","server":"127.0.0.1","port":last.port(),"username":"u","password":"p"}}
            ],
            "outbound_groups":[
                {"tag":"chain","type":"relay","proxies":["first","node"]},
                {"tag":"existing-tcp-use","type":"relay","proxies":["node","first"]}
            ],
            "route":{"final":{"type":"route","outbound":"chain"}}
        }).to_string()).unwrap();
        let proxy = Proxy::new(config).unwrap();
        let config = proxy.engine().runtime_snapshot().config().clone();
        proxy.protocols.start_components(config.clone(), deadline()).await.unwrap();
        proxy.protocols.commit_components(&config, || Ok(())).unwrap();
        assert!(proxy.tcp_runtime_services().components.as_ref().unwrap().configured("node").is_some(),
            "the TCP-only business suffix must share the same actual native pool");
        test_source::run(&proxy, |proxy, source| async move {
            let mut dispatch = source.new_dispatch("incoming").await.unwrap();
            let mut first_id = None;
            let mut total = 0;
            for payload in [b"relay-one".as_slice(), b"relay-two".as_slice()] {
                let id = dispatch.dispatch(UdpPipeInput {
                    peer_identity: None, target: Address::Ipv4([203,0,113,1]), port:443, payload,
                    route_target: None, sniffed_original_target: None, sniffed_host_source: None,
                    skip_fake_ip_restore: false, protocol: ProtocolType::new("test"), auth: None,
                    source_addr: None, client_session_id: None, transparent_target: false,
                    transparent_original_target: None, transparent_host_source: None, domain_memory: None,
                }).await.unwrap();
                if let Some(previous) = first_id { assert_eq!(id, previous); }
                first_id = Some(id);
                let (_, tasks, _) = dispatch.poll_sockets();
                let packet = tasks.join_next_with_id().await.unwrap().unwrap().unwrap();
                assert_eq!(packet.0, Address::Ipv4([203,0,113,1]));
                assert_eq!(packet.1, 443);
                assert_eq!(packet.2.as_ref(), payload);
                crate::runtime::udp_delivery::record_chain_udp_response_parts(
                    &proxy.udp_runtime_services(), &dispatch, packet.0, packet.1, packet.2, packet.3,
                );
                total += payload.len() as u64;
                let active = proxy.engine().active_sessions().into_iter().find(|session| session.id==id).unwrap();
                assert_eq!(active.outbound_tag.as_deref(), Some("node"));
                assert_eq!((active.outbound_tx_bytes, active.outbound_rx_bytes), (total,total));
            }
            dispatch.finish_all();
        }).await;
        assert_eq!(server.accepts.load(Ordering::SeqCst), 1);
        retiring.cancel();
        proxy.protocols.shutdown_components(deadline()).await.unwrap();
        server.close().await;
        peer.await.unwrap();
    }).await.unwrap();
}

#[tokio::test]
async fn ordinary_mieru_owned_udp_handoff_preserves_both_underlays_reuse_and_payload_accounting() {
    tokio::time::timeout(Duration::from_secs(20), async {
        for udp in [false, true] {
            let server = if udp {
                Server::packet(false).await
            } else {
                Server::new(false).await
            };
            let mut config = zero_config::RuntimeConfig::parse(
                &serde_json::json!({
                    "outbounds":[{"tag":"node","protocol":{"type":"mieru","server":"127.0.0.1",
                        "port":server.address.port(),"username":"u","password":"p"}}],
                    "route":{"final":{"type":"route","outbound":"node"}}
                })
                .to_string(),
            )
            .unwrap();
            if udp {
                let zero_config::OutboundProtocolConfig::Mieru { transport, .. } =
                    &mut config.outbounds[0].protocol
                else {
                    unreachable!()
                };
                *transport = zero_config::MieruTransport::Udp;
            }
            config.outbounds.push(
                serde_json::from_value(serde_json::json!({
                    "tag":"plain", "protocol":{"type":"direct"}
                }))
                .unwrap(),
            );
            // A separate business alias cannot demote this root's TCP/UDP pool.
            config.outbound_groups.push(
                serde_json::from_value(serde_json::json!({
                    "tag":"unused-branch", "type":"relay", "proxies":["node","plain"]
                }))
                .unwrap(),
            );
            let proxy = Proxy::new(config).unwrap();
            let config = proxy.engine().runtime_snapshot().config().clone();
            proxy
                .protocols
                .start_components(config.clone(), deadline())
                .await
                .unwrap();
            proxy
                .protocols
                .commit_components(&config, || Ok(()))
                .unwrap();
            assert!(
                proxy
                    .tcp_runtime_services()
                    .components
                    .as_ref()
                    .unwrap()
                    .configured("node")
                    .is_some(),
                "ordinary UDP must execute the same actual Mieru Datagram generation"
            );
            test_source::run(&proxy, |proxy, source| async move {
                let mut dispatch = source.new_dispatch("incoming").await.unwrap();
                let mut total = 0;
                let mut previous = None;
                for payload in [
                    b"owned-one".as_slice(),
                    b"".as_slice(),
                    b"owned-two".as_slice(),
                ] {
                    let id = dispatch
                        .dispatch(UdpPipeInput {
                            peer_identity: None,
                            target: Address::Ipv4([203, 0, 113, 1]),
                            port: 443,
                            payload,
                            route_target: None,
                            sniffed_original_target: None,
                            sniffed_host_source: None,
                            skip_fake_ip_restore: false,
                            protocol: ProtocolType::new("test"),
                            auth: None,
                            source_addr: None,
                            client_session_id: None,
                            transparent_target: false,
                            transparent_original_target: None,
                            transparent_host_source: None,
                            domain_memory: None,
                        })
                        .await
                        .unwrap();
                    if let Some(previous) = previous {
                        assert_eq!(id, previous);
                    }
                    previous = Some(id);
                    let (_, tasks, _) = dispatch.poll_sockets();
                    let packet = tasks.join_next_with_id().await.unwrap().unwrap().unwrap();
                    assert_eq!(packet.0, Address::Ipv4([203, 0, 113, 1]));
                    assert_eq!(packet.1, 443);
                    assert_eq!(packet.2.as_ref(), payload);
                    crate::runtime::udp_delivery::record_chain_udp_response_parts(
                        &proxy.udp_runtime_services(),
                        &dispatch,
                        packet.0,
                        packet.1,
                        packet.2,
                        packet.3,
                    );
                    total += payload.len() as u64;
                    let active = proxy
                        .engine()
                        .active_sessions()
                        .into_iter()
                        .find(|session| session.id == id)
                        .unwrap();
                    assert_eq!(active.outbound_tag.as_deref(), Some("node"));
                    assert_eq!(active.outbound_tx_bytes, total);
                    assert_eq!(active.outbound_rx_bytes, total);
                }
                dispatch.finish_all();
            })
            .await;
            assert_eq!(
                server.accepts.load(Ordering::SeqCst),
                1,
                "same UDP flow must reuse its actual carrier"
            );
            proxy
                .protocols
                .shutdown_components(deadline())
                .await
                .unwrap();
            server.close().await;
        }
    })
    .await
    .unwrap();
}

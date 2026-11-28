//! End-to-end raw-IP gateway coverage for domain-named nodes, pre-publication
//! TCP failures, proxy UDP `DatagramFlow`, Fake-IP Direct UDP, and `Block`,
//! plus unfinished resource detail reporting during `InboundOwner::shutdown`.
use super::*;

#[tokio::test]
async fn inbound_shutdown_surfaces_exact_unfinished_resource_detail() {
    use crate::runtime::{inbound_owners::InboundOwnerRegistry, Proxy};
    let proxy = Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap();
    proxy
        .protocols
        .drive_component_supervision(async {
            let config = proxy.engine().runtime_snapshot().config().clone();
            proxy
                .protocols
                .start_components(config.clone(), Instant::now() + Duration::from_secs(3))
                .await
                .unwrap();
            proxy
                .protocols
                .commit_components(&config, || Ok(()))
                .unwrap();
            let registry = Arc::new(InboundOwnerRegistry::default());
            let (stop, _) = watch::channel(false);
            let installed = registry
                .install("unconfirmed-detail", &proxy.protocols, 1, stop)
                .unwrap();
            let native = NativeObligations::new(1, 0, 0);
            installed.register_after_scope(native.resource()).unwrap();
            let mut permit = native.reserve(ChildKind::Tcp).unwrap();
            permit.enter().unwrap();
            drop(permit);
            let (stop_driver, driver_stopped) = oneshot::channel();
            installed
                .owner()
                .spawn_driver(async move {
                    let _ = driver_stopped.await;
                    Ok(())
                })
                .unwrap();
            installed.owner().seal_listener_installation();
            installed.owner().request_stop();
            let _ = stop_driver.send(());
            let _ = installed.owner().observe_driver().await;
            let error = registry
                .confirm_shutdown(
                    "unconfirmed-detail",
                    Instant::now() + Duration::from_millis(200),
                )
                .await
                .expect_err(
                    "entered obligation without gateway proof must keep cleanup unconfirmed",
                );
            let mut text = error.to_string();
            let mut cursor = std::error::Error::source(&error);
            while let Some(next) = cursor {
                text.push_str(": ");
                text.push_str(&next.to_string());
                cursor = next.source();
            }
            assert!(
                text.contains("inbound carrier cleanup remains unconfirmed:")
                    && text.contains("raw-IP child obligations"),
                "expected detailed unfinished message, got: {text}"
            );
            proxy
                .protocols
                .shutdown_components(Instant::now() + Duration::from_secs(2))
                .await
                .unwrap();
        })
        .await;
}

#[cfg(all(feature = "shadowsocks", feature = "dns"))]
#[tokio::test]
async fn raw_ip_domain_node_tcp_prepub_failure_udp_datagram_flow_fake_ip_direct_and_block_settle_cleanly(
) {
    use crate::runtime::{
        inbound_owners::InboundOwnerRegistry,
        route_runtime::{InboundRouteRuntimeFactory, SharedIngressRuntimeServices},
        tcp_ingress::NoClientResponseStreamProtocol,
        Proxy,
    };
    use crate::transport::ReplayStream;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpSocket, UdpSocket},
    };
    use zero_core::{Address, InboundUdpDispatch, Network, ProtocolType, Session};

    const KEY: &str = "AAECAwQFBgcICQoLDA0ODw==";

    timeout(Duration::from_secs(12), async {
        // 1. Authoritative UDP DNS server resolving `ss.node.test` and `real.direct.test` -> 127.0.0.1
        let dns_socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let dns_port = dns_socket.local_addr().unwrap().port();
        let (dns_stop_tx, mut dns_stop_rx) = oneshot::channel::<()>();
        let dns_task = {
            let socket = dns_socket.clone();
            tokio::spawn(async move {
                let mut buf = [0u8; 512];
                loop {
                    let (len, peer) = tokio::select! {
                        _ = &mut dns_stop_rx => break,
                        received = socket.recv_from(&mut buf) => match received {
                            Ok(pair) => pair,
                            Err(_) => break,
                        },
                    };
                    if len < 12 {
                        continue;
                    }
                    let mut pos = 12;
                    while pos < len && buf[pos] != 0 {
                        pos += 1 + usize::from(buf[pos]);
                    }
                    if pos + 5 > len {
                        continue;
                    }
                    let qtype = u16::from_be_bytes([buf[pos + 1], buf[pos + 2]]);
                    let qend = pos + 5;
                    let mut resp = Vec::with_capacity(qend + 16);
                    resp.extend_from_slice(&buf[0..2]);
                    resp.extend_from_slice(&[0x81, 0x80, 0x00, 0x01]);
                    if qtype == 1 {
                        resp.extend_from_slice(&[0x00, 0x01, 0x00, 0x00, 0x00, 0x00]);
                        resp.extend_from_slice(&buf[12..qend]);
                        resp.extend_from_slice(&[
                            0xc0, 0x0c, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x3c, 0x00,
                            0x04, 127, 0, 0, 1,
                        ]);
                    } else {
                        resp.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
                        resp.extend_from_slice(&buf[12..qend]);
                    }
                    let _ = socket.send_to(&resp, peer).await;
                }
            })
        };

        // 2. TCP + UDP echo upstreams
        let tcp_echo_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let tcp_echo_addr = tcp_echo_listener.local_addr().unwrap();
        let tcp_echo_task = tokio::spawn(async move {
            let (mut io, _) = tcp_echo_listener.accept().await.unwrap();
            let mut payload = Vec::new();
            io.read_to_end(&mut payload).await.unwrap();
            io.write_all(&payload).await.unwrap();
            io.shutdown().await.unwrap();
        });

        let udp_echo_socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let udp_echo_addr = udp_echo_socket.local_addr().unwrap();
        let (udp_echo_stop_tx, mut udp_echo_stop_rx) = oneshot::channel::<()>();
        let udp_echo_task = {
            let socket = udp_echo_socket.clone();
            tokio::spawn(async move {
                let mut buf = [0u8; 256];
                loop {
                    tokio::select! {
                        _ = &mut udp_echo_stop_rx => break,
                        received = socket.recv_from(&mut buf) => {
                            let Ok((len, peer)) = received else { break };
                            let _ = socket.send_to(&buf[..len], peer).await;
                        }
                    }
                }
            })
        };

        // 3. Real Shadowsocks server (TCP + UDP) + closed port for pre-publication failure
        let refusing = TcpSocket::new_v4().unwrap();
        refusing.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let refused_ss_port = refusing.local_addr().unwrap().port();
        drop(refusing);

        let ss_server_cfg = zero_config::RuntimeConfig::parse(
            &serde_json::json!({
                "inbounds": [{
                    "tag": "ss-in",
                    "listen": { "address": "127.0.0.1", "port": 0 },
                    "udp": { "enabled": true },
                    "protocol": {
                        "type": "shadowsocks",
                        "cipher": "2022-blake3-aes-128-gcm",
                        "password": KEY
                    }
                }],
                "route": { "final": { "type": "direct" } }
            })
            .to_string(),
        )
        .unwrap();
        let ss_server = crate::Proxy::new(ss_server_cfg.clone()).unwrap();
        let ss_registry = ss_server.system_tun_source_registry();
        let ss_running = ss_server.spawn();
        let ss_original = timeout(Duration::from_secs(5), async {
            loop {
                if let Some(owner) = ss_registry.owner("ss-in") {
                    break owner;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        ss_original.wait_ready().await.unwrap();
        let ss_handle = crate::ProxyHandle::new(
            zero_engine::EngineHandle::new(ss_server.engine().clone()),
            ss_server.clone(),
        );
        ss_handle
            .apply_runtime_config_and_wait(ss_server_cfg, Duration::from_secs(5))
            .await
            .unwrap();
        let ss_port = ss_original.original_listener_address.get().unwrap().port();

        // 4. Client Proxy with domain-named Shadowsocks outbounds, Block rule, and Fake-IP Direct rule
        let client_cfg = zero_config::RuntimeConfig::parse(
            &serde_json::json!({
                "runtime": {
                    "dns": {
                        "servers": {
                            "node": { "type": "udp", "host": "127.0.0.1", "port": dns_port },
                            "query": { "type": "udp", "host": "127.0.0.1", "port": dns_port }
                        },
                        "default_server": "query",
                        "policy": {
                            "node_server": "node",
                            "address_family": "ipv4_only"
                        }
                    }
                },
                "inbounds": [{
                    "tag": "tun-coverage",
                    "listen": { "address": "127.0.0.1", "port": 0 },
                    "udp": { "enabled": true },
                    "protocol": { "type": "socks5" }
                }],
                "outbounds": [
                    {
                        "tag": "ss-live",
                        "protocol": {
                            "type": "shadowsocks",
                            "server": "ss.node.test",
                            "port": ss_port,
                            "cipher": "2022-blake3-aes-128-gcm",
                            "password": KEY
                        }
                    },
                    {
                        "tag": "ss-dead",
                        "protocol": {
                            "type": "shadowsocks",
                            "server": "ss.node.test",
                            "port": refused_ss_port,
                            "cipher": "2022-blake3-aes-128-gcm",
                            "password": KEY
                        }
                    }
                ],
                "route": {
                    "rules": [
                        {
                            "condition": { "type": "domain_keyword", "values": ["blocked.test"] },
                            "action": { "type": "reject" }
                        },
                        {
                            "condition": { "type": "domain_keyword", "values": ["dead.test"] },
                            "action": { "type": "route", "outbound": "ss-dead" }
                        },
                        {
                            "condition": { "type": "domain_keyword", "values": ["direct.test"] },
                            "action": { "type": "direct" }
                        }
                    ],
                    "final": { "type": "route", "outbound": "ss-live" }
                }
            })
            .to_string(),
        )
        .unwrap();

        let client = Proxy::new(client_cfg.clone()).unwrap();
        let client_running = client.spawn();
        let client_handle = crate::ProxyHandle::new(
            zero_engine::EngineHandle::new(client.engine().clone()),
            client.clone(),
        );
        client_handle
            .apply_runtime_config_and_wait(client_cfg, Duration::from_secs(5))
            .await
            .unwrap();
        {
            let config = client.engine().runtime_snapshot().config().clone();

            let registry = Arc::new(InboundOwnerRegistry::default());
            let (stop, _) = watch::channel(false);
            let installed = registry
                .install("tun-coverage", &client.protocols, 6, stop)
                .unwrap();
            let group = installed
                .child_tasks(NonZeroUsize::new(6).unwrap())
                .unwrap();
            let native = NativeObligations::new(4, 4, 4);
            native
                .bind_source(installed.source_registrar().scope_identity())
                .unwrap();
            installed.register_after_scope(native.resource()).unwrap();
            let owner = installed.owner();
            let host = installed.tasks();
            let (stop_parent, parent_stopped) = oneshot::channel();
                owner
                    .spawn_driver(async move {
                        let _ = parent_stopped.await;
                        Ok(())
                    })
                    .unwrap();
                owner.seal_listener_installation();
                let monitor = {
                    let owner = owner.clone();
                    tokio::spawn(async move { owner.observe_driver().await })
                };

                let factory = InboundRouteRuntimeFactory::new(
                    SharedIngressRuntimeServices::new(client.tcp_runtime_services()),
                    "tun-coverage".into(),
                );

                // Helper to run one raw-IP TCP child task on `group` + `native`
                let run_raw_tcp = |mut session: Session, payload: &'static [u8]| {
                    let group = group.clone();
                    let host = host.clone();
                    let native = native.clone();
                    let runtime = factory.for_connection(Some("10.77.0.2:50000".parse().unwrap()));
                    async move {
                        let mut permit = native
                            .reserve_reclaimed(
                                ChildKind::Tcp,
                                Instant::now() + Duration::from_secs(3),
                            )
                            .await
                            .unwrap();
                        let (mut client_io, server_io) = tokio::io::duplex(1024);
                        let (done_tx, done_rx) = oneshot::channel();
                        session.transparent_target = true;
                        assert!(group
                            .spawn_bound(&host, move |binding| async move {
                                binding.wait_bound().await.map_err(std::io::Error::other)?;
                                permit.bind_caller(binding).map_err(std::io::Error::other)?;
                                let runtime = runtime.with_caller_native(permit.registration());
                                permit.enter().map_err(std::io::Error::other)?;
                                let protocol = NoClientResponseStreamProtocol::new();
                                let outcome = runtime
                                    .serve(
                                        session,
                                        ReplayStream::new(server_io, Vec::new()),
                                        &protocol,
                                    )
                                    .await;
                                permit.registration().note_no_gateway_opened();
                                let res = permit.consume_business_result(outcome);
                                let _ = done_tx.send(());
                                res
                            })
                            .unwrap());
                        if !payload.is_empty() {
                            client_io.write_all(payload).await.unwrap();
                            client_io.shutdown().await.unwrap();
                            let mut echoed = Vec::new();
                            let _ = client_io.read_to_end(&mut echoed).await;
                            done_rx.await.unwrap();
                            echoed
                        } else {
                            drop(client_io);
                            done_rx.await.unwrap();
                            Vec::new()
                        }
                    }
                };

                // (a) Domain-sniffed TCP `Block` rule
                let mut blocked_tcp = Session::new(
                    0,
                    Address::Domain("ads.blocked.test".into()),
                    443,
                    Network::Tcp,
                    ProtocolType::UNKNOWN,
                );
                blocked_tcp.direct_target = None;
                let _ = run_raw_tcp(blocked_tcp, b"").await;

                // (b) Domain-named Shadowsocks pre-publication failure (`ss-dead`)
                let dead_tcp = Session::new(
                    0,
                    Address::Domain("app.dead.test".into()),
                    tcp_echo_addr.port(),
                    Network::Tcp,
                    ProtocolType::UNKNOWN,
                );
                let _ = run_raw_tcp(dead_tcp, b"").await;

                // (c) Domain-named Shadowsocks live TCP relay (`ss-live` -> `ss.node.test`)
                let live_tcp = Session::new(
                    0,
                    Address::Ipv4([127, 0, 0, 1]),
                    tcp_echo_addr.port(),
                    Network::Tcp,
                    ProtocolType::UNKNOWN,
                );
                let echoed_tcp = run_raw_tcp(live_tcp, b"hello-ss-tcp").await;
                assert_eq!(echoed_tcp, b"hello-ss-tcp");
                tcp_echo_task.await.unwrap();

                // (d) Raw-IP UDP association: Block + Shadowsocks `DatagramFlow` + Fake-IP Direct UDP
                let mut udp_permit = native
                    .reserve_reclaimed(ChildKind::Udp, Instant::now() + Duration::from_secs(3))
                    .await
                    .unwrap();
                let returns = Arc::new(crate::component::datagram_retirement::DatagramReturns::default());
                let udp_runtime = crate::runtime::udp_ingress::UdpIngressRuntime::new(
                    client
                        .tcp_runtime_services()
                        .for_inbound("tun-coverage".into()),
                )
                .with_datagram_returns(returns.clone())
                .with_source_addr(Some("10.77.0.2:53000".parse().unwrap()));
                let ingress_role = udp_runtime.peer_ingress().unwrap();
                let (udp_done_tx, udp_done_rx) = oneshot::channel();
                {
                    let group = group.clone();
                    let group_for_ctx = group.clone();
                    assert!(group
                        .spawn_bound(&host, move |binding| async move {
                            binding.wait_bound().await.map_err(std::io::Error::other)?;
                            udp_permit
                                .bind_caller(binding.clone())
                                .map_err(std::io::Error::other)?;
                            let (slot, job) = crate::runtime::udp_socket::endpoint::peer::DirectPeerDriverSlot::channel();
                            let budget = crate::runtime::udp_dispatch::BoundDirectBudget::for_accepted(&config);
                            let udp_runtime = udp_runtime
                                .with_peer_resources(
                                    group_for_ctx.resource_context(binding, ingress_role),
                                )
                                .with_direct_peer_driver(slot)
                                .with_native(Some(udp_permit.registration()))
                                .with_bound_direct_budget(budget);
                            udp_permit.enter().map_err(std::io::Error::other)?;

                            let work = async {
                                let mut dispatch =
                                    udp_runtime.new_dispatch("tun-coverage").await?;
                                // 1. Block UDP domain target
                                let blocked_pkt = InboundUdpDispatch::new(
                                    ProtocolType::UNKNOWN,
                                    Address::Domain("ads.blocked.test".into()),
                                    443,
                                    b"drop".to_vec(),
                                    None,
                                );
                                let _ = udp_runtime
                                    .dispatch_inbound_packet(
                                        &mut dispatch,
                                        &blocked_pkt,
                                        None,
                                        Some("10.77.0.2:53000".parse().unwrap()),
                                    )
                                    .await;

                                // 2. Proxy UDP `DatagramFlow` via `ss-live` (`ss.node.test`)
                                let ss_pkt = InboundUdpDispatch::new(
                                    ProtocolType::UNKNOWN,
                                    Address::Ipv4([127, 0, 0, 1]),
                                    udp_echo_addr.port(),
                                    b"hello-ss-udp".to_vec(),
                                    None,
                                );
                                udp_runtime
                                    .dispatch_inbound_packet(
                                        &mut dispatch,
                                        &ss_pkt,
                                        None,
                                        Some("10.77.0.2:53000".parse().unwrap()),
                                    )
                                    .await?;

                                // 3. Fake-IP / domain Direct UDP (`real.direct.test`, `direct_target = None`)
                                let direct_pkt = InboundUdpDispatch::new(
                                    ProtocolType::UNKNOWN,
                                    Address::Domain("real.direct.test".into()),
                                    udp_echo_addr.port(),
                                    b"hello-direct-udp".to_vec(),
                                    None,
                                );
                                udp_runtime
                                    .dispatch_inbound_packet(
                                        &mut dispatch,
                                        &direct_pkt,
                                        None,
                                        Some("10.77.0.2:53000".parse().unwrap()),
                                    )
                                    .await?;
                                drop(dispatch);
                                Ok::<(), zero_engine::EngineError>(())
                            };
                            let outcome =
                                crate::runtime::udp_socket::endpoint::peer::job::run_peer_preserving(
                                    Box::pin(work),
                                    job,
                                )
                                .await;
                            udp_permit.registration().note_no_gateway_opened();
                            let reported = outcome
                                .as_ref()
                                .map(|_| ())
                                .map_err(|err| format!("{err:#?}"));
                            let res = outcome.map_err(|err| {
                                zero_engine::EngineError::Io(udp_permit.retain_error(err))
                            });
                            let _ = udp_done_tx.send(reported);
                            res
                        })
                        .unwrap());
                }
                udp_done_rx
                    .await
                    .unwrap()
                    .expect("raw-IP UDP Block + DatagramFlow + Fake-IP Direct must succeed");

                assert!(
                    native
                        .0
                        .lock()
                        .unwrap()
                        .records
                        .values()
                        .all(|record| !record.uncovered_gateway),
                    "none of the raw-IP gateways may be marked uncovered"
                );

                owner.request_stop();
                stop_parent.send(()).unwrap();
                monitor.await.unwrap().unwrap();
                registry
                    .confirm_shutdown("tun-coverage", Instant::now() + Duration::from_secs(3))
                    .await
                    .expect("all raw-IP TCP/UDP/DNS obligations must settle cleanly on shutdown");
                assert!(native.0.lock().unwrap().records.is_empty());
                assert!(owner.terminal_for_test().is_some());

                let _ = client
                    .tcp_runtime_services()
                    .execution()
                    .retired_datagrams()
                    .wait_returns(&returns, Instant::now() + Duration::from_secs(2))
                    .await
                    .unwrap();
        }

        client_running.shutdown().await.unwrap();
        ss_running.shutdown().await.unwrap();
        let _ = dns_stop_tx.send(());
        let _ = udp_echo_stop_tx.send(());
        let _ = dns_task.await;
        let _ = udp_echo_task.await;
    })
    .await
    .unwrap();
}

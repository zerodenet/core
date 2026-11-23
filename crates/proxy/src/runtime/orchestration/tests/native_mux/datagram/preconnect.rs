//! Existing TCP/UDP wire modes use one actual warming entry and original ledger.
use super::*;
use crate::runtime::udp_ingress::test_source;
#[tokio::test]
async fn vless_preconnect_business_and_mux_callers_share_host_custody() {
    timeout(Duration::from_secs(50), async {
        for (tcp_mux, udp_mux) in [
            (None, None),
            (Some(2), None),
            (None, Some(2)),
            (Some(2), Some(2)),
        ] {
            let server = Server::start("vless").await;
            let udp = UdpEcho::start().await;
            let mut candidate = config("vless", server.fixture.front.port());
            candidate.runtime.native_stream_max_originals = Some(2);
            let zero_config::OutboundProtocolConfig::Vless {
                mux_concurrency,
                xudp_concurrency,
                testpre,
                ..
            } = &mut candidate.outbounds[0].protocol
            else {
                unreachable!()
            };
            *mux_concurrency = tcp_mux;
            *xudp_concurrency = udp_mux;
            *testpre = 1;
            let proxy = Proxy::new(candidate).unwrap();
            let running = start_components(&proxy).await;
            let mut tcp = connect(&proxy, server.fixture.echo.port()).await.unwrap();
            echo(&mut tcp).await;
            let unchanged = (*proxy.engine().config()).clone();
            proxy.protocols.on_config_reloaded(&unchanged);
            echo(&mut tcp).await;
            let target = udp.target();
            let (owners, originals, drivers) =
                test_source::run(&proxy, move |proxy, runtime| async move {
                    let (association, mut channel) =
                        open_admitted(&proxy, &runtime, target.clone())
                            .await
                            .unwrap_or_else(|error| {
                                panic!("TCP MUX {tcp_mux:?}, XUDP {udp_mux:?}: {error:?}")
                            });
                    send_packet(
                        &mut channel,
                        association,
                        target.clone(),
                        b"owned-preconnect",
                    )
                    .await;
                    receive(&mut channel, association, target, b"owned-preconnect").await;
                    let services = proxy.tcp_runtime_services();
                    let plan = services.components.as_ref().unwrap();
                    let root = plan.configured("a").unwrap();
                    assert!(root.recipe.spec().resources.is_empty());
                    assert!(root.recipe.spec().task_limit.is_none());
                    let mut pending = root
                        .dependency_instances()
                        .iter()
                        .map(|id| id.component)
                        .collect::<Vec<_>>();
                    let mut seen = std::collections::BTreeSet::new();
                    let mut owners = Vec::new();
                    while let Some(id) = pending.pop() {
                        if !seen.insert(id) {
                            continue;
                        }
                        let slot = plan.component(id).unwrap();
                        pending.extend(slot.dependency_instances().iter().map(|id| id.component));
                        for ordinal in 0..slot.recipe.spec().resources.len() {
                            if let Some(owner) = slot.original_carrier_owner(ordinal) {
                                owners.push(owner);
                            }
                        }
                    }
                    assert_eq!(
                        owners.len(),
                        if tcp_mux.is_some() || udp_mux.is_some() {
                            2
                        } else {
                            1
                        }
                    );
                    let mut originals = Vec::new();
                    let mut drivers = Vec::new();
                    let mut native = 0;
                    for owner in &owners {
                        let observed = owner.observe().unwrap();
                        assert!(observed.slots().len() <= owner.limit().get());
                        for slot in observed.slots() {
                            native += usize::from(slot.native().is_some());
                            originals.push(slot.original().clone());
                            if let Some((_, driver)) = slot.driver() {
                                drivers.push(driver.clone());
                            }
                        }
                    }
                    assert!(native >= 1);

                    let zero_platform_tokio::contract::datagram::DatagramSession {
                        tx,
                        rx,
                        mut control,
                        ..
                    } = channel;
                    drop(tx);
                    drop(rx);
                    assert!(control.wait_closed(deadline()).await.unwrap().is_complete());
                    (owners, originals, drivers)
                })
                .await;
            echo(&mut tcp).await;
            drop(tcp);
            proxy
                .protocols
                .shutdown_components(deadline())
                .await
                .unwrap();
            for original in originals {
                assert!(original.matches_closed(&original.closed().unwrap()));
            }
            for driver in drivers {
                let joined = driver.try_joined().unwrap();
                assert!(driver.matches_joined(&joined) && joined.registration_released());
            }
            for owner in owners {
                assert!(owner.closed().unwrap().matches(&owner));
                assert!(owner.users_closed().unwrap().matches(&owner));
            }
            running.shutdown().await.unwrap();
            udp.close().await;
            server.close().await;
        }
    })
    .await
    .unwrap();
}

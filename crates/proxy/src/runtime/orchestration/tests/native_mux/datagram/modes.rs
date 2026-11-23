use super::*;
use crate::runtime::udp_ingress::test_source;

// Both wire grammars predate the migration; these are not new carrier pairs.
#[tokio::test]
async fn vless_independent_tcp_and_udp_modes_keep_their_wire_and_confirm_every_original() {
    timeout(Duration::from_secs(30), async {
        for (tcp_mux, udp_mux) in [(Some(2), None), (None, Some(3)), (Some(2), Some(3))] {
            let server = Server::start("vless").await;
            let udp = UdpEcho::start().await;
            let mut candidate = config("vless", server.fixture.front.port());
            let zero_config::OutboundProtocolConfig::Vless { mux_concurrency, xudp_concurrency, .. } = &mut candidate.outbounds[0].protocol else {
                unreachable!()
            };
            *mux_concurrency = tcp_mux;
            *xudp_concurrency = udp_mux;
            let proxy = Proxy::new(candidate).unwrap();
            let running = start_components(&proxy).await;
            let tcp_port = server.fixture.echo.port();
            let target = udp.target();
            let mixed = tcp_mux.is_none() || udp_mux.is_none();
            let (owners, originals, drivers) = test_source::run(&proxy, move |proxy, runtime| async move {
                let tcp_runtime = runtime.tcp_runtime("incoming");
                let mut session = Session::new(
                    0,
                    Address::Ipv4([127, 0, 0, 1]),
                    tcp_port,
                    Network::Tcp,
                    ProtocolType::new("native-mux-test"),
                );
                let ticket = tcp_runtime.prepare_session_with_ticket(&mut session, None).await.unwrap();
                let demand = crate::component::business_demand::AdmittedBusinessDemand::new(
                    proxy.engine(),
                    zero_core::component::SessionId(session.id),
                    ticket,
                ).unwrap();
                let connected = crate::runtime::tcp_dispatch::dispatch_tcp(
                    &tcp_runtime,
                    &mut session,
                    Some(demand),
                ).await.unwrap();
                assert_eq!(connected.outbound_tag, "a");
                let mut tcp = connected.upstream;
                echo(&mut tcp).await;

                let (association, mut channel) = open_admitted(&proxy, &runtime, target.clone())
                    .await
                    .unwrap_or_else(|error| panic!("TCP MUX {tcp_mux:?}, XUDP {udp_mux:?}: {error:?}"));
                send_packet(&mut channel, association, target.clone(), b"independent-mode").await;
                receive(&mut channel, association, target, b"independent-mode").await;
                let services = proxy.tcp_runtime_services();
                let plan = services.components.as_ref().unwrap();
                let slot = plan.configured("a").unwrap();
                let mut owners = vec![registration(&proxy)];
                for (ordinal, resource) in slot.recipe.spec().resources.iter().enumerate() {
                    if matches!(resource, zero_platform_tokio::contract::component::ResourceDeclaration::ControlledNodeCarrier { .. }) {
                        owners.push(slot.original_carrier_owner(ordinal).unwrap());
                    }
                }
                assert_eq!(owners.len(), if mixed { 2 } else { 1 });
                let mut originals = Vec::new();
                let mut drivers = Vec::new();
                for owner in &owners {
                    let observed = owner.observe().unwrap();
                    for physical in observed.slots() {
                        originals.push(physical.original().clone());
                        if let Some((_, driver)) = physical.driver() {
                            drivers.push(driver.clone());
                        }
                    }
                }
                assert_eq!(originals.len(), if mixed { 2 } else { 1 });

                let zero_platform_tokio::contract::datagram::DatagramSession { tx, rx, mut control, .. } = channel;
                drop(tx);
                drop(rx);
                assert!(control.wait_closed(deadline()).await.unwrap().is_complete());
                echo(&mut tcp).await;
                drop(tcp);
                tcp_runtime
                    .track_session(session.id)
                    .finish(zero_engine::SessionOutcome::ChainedRelayed)
                    .unwrap();
                (owners, originals, drivers)
            }).await;
            assert_eq!(server.fixture.connections(), if mixed { 2 } else { 1 });
            proxy.protocols.shutdown_components(deadline()).await.unwrap();
            for original in originals {
                let receipt = original.closed().expect("same actual native/borrowed original must close");
                assert!(original.matches_closed(&receipt));
            }
            for driver in drivers {
                assert!(driver.matches_joined(&driver.try_joined().unwrap()));
            }
            for owner in owners {
                assert!(owner.closed().unwrap().matches(&owner));
                assert!(owner.users_closed().unwrap().matches(&owner));
            }
            running.shutdown().await.unwrap();
            udp.close().await;
            server.close().await;
        }
    }).await.unwrap();
}

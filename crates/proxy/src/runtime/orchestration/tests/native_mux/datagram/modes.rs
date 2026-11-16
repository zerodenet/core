use super::*;

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
            let mut tcp = connect(&proxy, server.fixture.echo.port()).await.unwrap();
            echo(&mut tcp).await;
            let mut channel = open(&proxy, 400, DatagramTargets::Fixed(udp.target())).await.unwrap();
            send_packet(&mut channel, 400, udp.target(), b"independent-mode").await;
            receive(&mut channel, 400, udp.target(), b"independent-mode").await;
            let services = proxy.tcp_runtime_services();
            let plan = services.components.as_ref().unwrap();
            let slot = plan.configured("a").unwrap();
            let mut owners = vec![registration(&proxy)];
            for (ordinal, resource) in slot.recipe.spec().resources.iter().enumerate() {
                if matches!(resource, zero_platform_tokio::contract::component::ResourceDeclaration::ControlledNodeCarrier { .. }) {
                    owners.push(slot.original_carrier_owner(ordinal).unwrap());
                }
            }
            let mixed = tcp_mux.is_none() || udp_mux.is_none();
            assert_eq!(owners.len(), if mixed { 2 } else { 1 });
            assert_eq!(server.fixture.connections(), if mixed { 2 } else { 1 });
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

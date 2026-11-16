//! Live policy and group decisions keep the same actual Source generation.
use super::*;
fn classify(live: &Fixture) -> PacketRouteTarget {
    let route = crate::runtime::route_runtime::InboundRouteRuntimeFactory::new(
        crate::runtime::route_runtime::SharedIngressRuntimeServices::new(
            live.proxy.tcp_runtime_services(),
        ),
        "tun-test".into(),
    );
    route.packet_route_target("10.0.0.2".parse().unwrap(), Some(IPPROTO_UDP))
}
#[tokio::test]
async fn live_global_selector_urltest_and_fallback_order_select_actual_providers() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let live = Fixture::start(true).await;
        let engine = live.proxy.engine();
        let revision = engine.config_revision();
        engine.set_mode(zero_config::ModeConfig::Direct);
        assert!(matches!(classify(&live), PacketRouteTarget::Flow));
        for (outbound, packet) in [("plain", false), ("wg", true), ("plain", false)] {
            engine.set_mode(zero_config::ModeConfig::Global {
                outbound: outbound.into(),
            });
            assert_eq!(
                matches!(classify(&live), PacketRouteTarget::Packet { .. }),
                packet
            );
        }
        engine.set_mode(zero_config::ModeConfig::Global {
            outbound: "choose".into(),
        });
        for (outbound, packet) in [("plain", false), ("wg", true), ("plain", false)] {
            engine.set_selector_target("choose", outbound).unwrap();
            assert_eq!(
                matches!(classify(&live), PacketRouteTarget::Packet { .. }),
                packet
            );
        }
        engine.set_mode(zero_config::ModeConfig::Global {
            outbound: "auto".into(),
        });
        let snapshot = engine.runtime_snapshot();
        let plan = snapshot.plan();
        let group_id = plan.target_id("auto").unwrap();
        let plain = plan.target_id("plain").unwrap();
        let wg = plan.target_id("wg").unwrap();
        let group = plan.target(group_id).unwrap().as_urltest().unwrap();
        for (selected, other, packet) in [(plain, wg, false), (wg, plain, true), (plain, wg, false)]
        {
            let selection = group.select(None, &[(selected, 1), (other, 1000)]);
            snapshot.update_urltest_state(group_id, selected, Some(1), vec![], selection);
            assert_eq!(
                matches!(classify(&live), PacketRouteTarget::Packet { .. }),
                packet
            );
        }
        engine.set_mode(zero_config::ModeConfig::Global {
            outbound: "balance".into(),
        });
        let mut orders = Vec::new();
        for _ in 0..4 {
            let candidates = classify(&live)
                .into_candidates()
                .into_iter()
                .map(|target| match target {
                    PacketRouteTarget::Packet { role, .. } => {
                        assert_eq!(role, live.plan().configured("wg").unwrap().role_id());
                        "packet"
                    }
                    PacketRouteTarget::Flow => "flow",
                    _ => panic!("actual Direct/WireGuard candidates required"),
                })
                .collect::<Vec<_>>();
            assert_eq!(candidates.len(), 2);
            orders.push(candidates);
        }
        assert_ne!(orders[0], orders[1]);
        assert_eq!(orders[0], orders[2]);
        assert_eq!(orders[1], orders[3]);
        assert_eq!(engine.config_revision(), revision);
        // Every policy change kept the same published provider generation.
        assert!(live
            .plan()
            .has_packet_provider(live.plan().configured("wg").unwrap().role_id()));
        drop(snapshot);
        live.running.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn warmed_native_selection_rechecks_endpoint_intents_without_rebuilding_the_provider() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let live = Fixture::start(true).await;
        let engine = live.proxy.engine();
        let original = engine.runtime_snapshot();
        let revision = engine.config_revision();
        let actual = live.plan().configured("wg").unwrap().role_id();
        let query = zero_api::EndpointGetQuery {
            endpoint_id: "endpoint:wg".into(),
        };
        let intent_revision = engine.endpoint_snapshot(&query).unwrap().intent_revision;
        for change in [
            zero_engine::EndpointChange::Enabled(false),
            zero_engine::EndpointChange::Directions(zero_api::EndpointDirections {
                inbound: true,
                outbound: false,
            }),
        ] {
            live.assert_packet(IPPROTO_UDP, RouteMode::Packet, false);
            let changed = engine
                .prepare_endpoint_change(
                    &original,
                    &query.endpoint_id,
                    change,
                    false,
                    Some(intent_revision),
                )
                .unwrap();
            assert!(Arc::ptr_eq(original.config(), changed.config()));
            engine.restore_staged_snapshot(changed, false).unwrap();
            assert!(matches!(classify(&live), PacketRouteTarget::Unsupported));
            engine
                .restore_staged_snapshot(original.clone(), false)
                .unwrap();
            live.assert_packet(IPPROTO_UDP, RouteMode::Packet, false);
            assert_eq!(live.plan().configured("wg").unwrap().role_id(), actual);
            assert_eq!(engine.config_revision(), revision);
        }
        live.running.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

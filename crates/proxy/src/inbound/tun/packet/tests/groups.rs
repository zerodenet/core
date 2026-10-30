use super::*;
use crate::inventory::PacketRouteTarget;
use zero_config::ModeConfig;

fn grouped() -> Value {
    json!({
        "outbounds":[
            {"tag":"plain","protocol":{"type":"direct"}},
            {"tag":"wg","protocol":{"type":"wireguard",
                "private_key":"01".repeat(32),"addresses":["10.222.0.2/32"],
                "peers":[{"public_key":"02".repeat(32),"endpoint":"127.0.0.1:51820",
                    "allowed_ips":["0.0.0.0/0","::/0"]}]}}
        ],
        "outbound_groups":[
            {"tag":"choose","type":"selector","outbounds":["plain","wg"],"selected":"plain"},
            {"tag":"auto","type":"url_test","outbounds":["plain","wg"]},
            {"tag":"balance","type":"load_balance","outbounds":["plain","wg"],"strategy":"round_robin"}
        ],
        "route":{"rules":[],"final":{"type":"route","outbound":"choose"}}
    })
}

#[tokio::test]
async fn warmed_routes_observe_rule_direct_and_different_global_targets() {
    let mut value = grouped();
    value["route"] = reject_packet()["route"].clone();
    let mut fixture = Fixture::new(value);
    let packet = datagram("203.0.113.1".parse().unwrap(), 443, b"payload");
    fixture.proxy.engine().set_mode(ModeConfig::Direct);
    assert!(!fixture.forward(&packet, 1500, false).await);
    fixture.proxy.engine().set_mode(ModeConfig::Rule);
    assert!(fixture.forward(&packet, 1500, false).await);
    for (outbound, handled) in [("plain", false), ("wg", true), ("plain", false)] {
        fixture.proxy.engine().set_mode(ModeConfig::Global {
            outbound: outbound.into(),
        });
        assert_eq!(fixture.forward(&packet, 1500, false).await, handled);
    }
    fixture.no_response();
}

#[tokio::test]
async fn warmed_selector_rechecks_packet_plane_without_migrating_a_flow_pin() {
    let mut fixture = Fixture::new(grouped());
    let packet = datagram("203.0.113.1".parse().unwrap(), 443, b"payload");
    let snapshot = fixture.proxy.engine().runtime_snapshot();
    let revision = fixture.proxy.engine().config_revision();
    assert!(!fixture.forward(&packet, 1500, false).await);
    fixture
        .proxy
        .engine()
        .set_selector_target("choose", "wg")
        .unwrap();
    assert!(matches!(
        fixture
            .route
            .packet_route_target("203.0.113.1".parse().unwrap(), Some(packet::IPPROTO_UDP)),
        PacketRouteTarget::Packet { .. }
    ));
    assert!(fixture.forward(&packet, 1500, false).await);
    fixture
        .proxy
        .engine()
        .set_selector_target("choose", "plain")
        .unwrap();
    assert!(!fixture.forward(&packet, 1500, false).await);
    assert_eq!(fixture.proxy.engine().config_revision(), revision);
    assert!(std::sync::Arc::ptr_eq(
        &snapshot,
        &fixture.proxy.engine().runtime_snapshot()
    ));
    fixture.no_response();
}

#[tokio::test]
async fn warmed_route_evaluation_keeps_urltest_and_round_robin_resolution_live() {
    let mut value = grouped();
    value["route"]["final"]["outbound"] = json!("auto");
    let mut fixture = Fixture::new(value);
    let destination = "203.0.113.1".parse().unwrap();
    let packet = datagram(destination, 443, b"payload");
    let snapshot = fixture.proxy.engine().runtime_snapshot();
    let revision = fixture.proxy.engine().config_revision();
    let plan = snapshot.plan();
    let group_id = plan.target_id("auto").unwrap();
    let plain = plan.target_id("plain").unwrap();
    let wireguard = plan.target_id("wg").unwrap();
    let group = plan.target(group_id).unwrap().as_urltest().unwrap();
    for (selected, other, handled) in [
        (plain, wireguard, false),
        (wireguard, plain, true),
        (plain, wireguard, false),
    ] {
        let selection = group.select(None, &[(selected, 1), (other, 1000)]);
        snapshot.update_urltest_state(group_id, selected, Some(1), vec![], selection);
        assert_eq!(fixture.forward(&packet, 1500, false).await, handled);
    }
    assert_eq!(fixture.proxy.engine().config_revision(), revision);
    assert!(std::sync::Arc::ptr_eq(
        &snapshot,
        &fixture.proxy.engine().runtime_snapshot()
    ));
    fixture.no_response();

    let mut value = grouped();
    value["route"]["final"]["outbound"] = json!("balance");
    let fixture = Fixture::new(value);
    let mut orders = Vec::new();
    for _ in 0..4 {
        let order = fixture
            .route
            .packet_route_target(destination, Some(packet::IPPROTO_UDP))
            .into_candidates()
            .into_iter()
            .map(|target| match target {
                PacketRouteTarget::Flow => "flow",
                PacketRouteTarget::Packet { .. } => "packet",
                _ => panic!("expected actual direct/WireGuard candidates"),
            })
            .collect::<Vec<_>>();
        assert_eq!(order.len(), 2);
        orders.push(order);
    }
    assert_ne!(orders[0], orders[1]);
    assert_eq!(orders[0], orders[2]);
    assert_eq!(orders[1], orders[3]);
}

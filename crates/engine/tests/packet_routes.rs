use zero_api::*;
use zero_config::RuntimeConfig;
use zero_engine::Engine;
fn engine() -> Engine {
    Engine::new(
        RuntimeConfig::parse(r#"{"route":{"rules":[],"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap()
}
fn descriptor(input: &str, output: &str) -> PacketRouteSnapshot {
    PacketRouteSnapshot {
        route_id: String::new(),
        core_instance_id: String::new(),
        config_revision: 0,
        inbound_tag: input.into(),
        outbound_tag: output.into(),
        endpoints: vec![],
        source: "10.0.0.2".into(),
        destination: "10.0.0.3".into(),
        ip_protocol: 17,
        translated: false,
        started_at_unix_ms: 0,
        state: PacketRouteState::Active,
    }
}
#[tokio::test]
async fn route_close_checks_instance_revision_and_waits_for_actual_owner_receipt() {
    let e = engine();
    let a = e.register_packet_route(descriptor("tun", "a")).unwrap();
    let b = e.register_packet_route(descriptor("tun", "b")).unwrap();
    let list = e.packet_routes_snapshot(&PacketRouteListQuery::default());
    assert_eq!(list.total, 2);
    let id = list
        .routes
        .iter()
        .find(|r| r.outbound_tag == "a")
        .unwrap()
        .route_id
        .clone();
    let mut c = PacketRouteCloseCommand {
        route_id: id.clone(),
        expected_core_instance_id: "old".into(),
        expected_config_revision: None,
    };
    assert_eq!(
        e.begin_close_packet_route(&c).err().unwrap().code,
        ApiErrorCode::Conflict
    );
    c.expected_core_instance_id = e.core_instance_id().into();
    c.expected_config_revision = Some(e.config_revision() + 1);
    assert_eq!(
        e.begin_close_packet_route(&c).err().unwrap().code,
        ApiErrorCode::Conflict
    );
    c.expected_config_revision = None;
    let (snapshot, closed) = e.begin_close_packet_route(&c).unwrap();
    assert_eq!(snapshot.state, PacketRouteState::Closing);
    assert!(!b.control().is_closed());
    assert_eq!(
        e.begin_close_packet_route(&c).err().unwrap().code,
        ApiErrorCode::Conflict
    );
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(5), closed.wait_released())
            .await
            .is_err()
    );
    drop(a);
    closed.wait_released().await;
    assert_eq!(
        e.packet_routes_snapshot(&PacketRouteListQuery::default())
            .total,
        1
    );
    assert_eq!(
        e.packet_route_snapshot(&PacketRouteGetQuery { route_id: id })
            .unwrap_err()
            .code,
        ApiErrorCode::NotFound
    );
    drop(b);
    assert_eq!(
        e.packet_routes_snapshot(&PacketRouteListQuery::default())
            .total,
        0
    );
}
#[test]
fn packet_route_queries_paginate_and_do_not_include_flow_connections() {
    let e = engine();
    let _a = e.register_packet_route(descriptor("tun", "a")).unwrap();
    let _b = e.register_packet_route(descriptor("wg", "b")).unwrap();
    let page = e.packet_routes_snapshot(&PacketRouteListQuery {
        limit: Some(1),
        ..Default::default()
    });
    assert_eq!(page.routes.len(), 1);
    assert_eq!(page.total, 2);
    assert_eq!(page.next_offset, Some(1));
    let page = e.packet_routes_snapshot(&PacketRouteListQuery {
        outbound_tag: Some("b".into()),
        ..Default::default()
    });
    assert_eq!(page.total, 1);
    assert_eq!(page.routes[0].inbound_tag, "wg");
    let q = QueryRequest::PacketRoute(PacketRouteGetQuery {
        route_id: page.routes[0].route_id.clone(),
    });
    assert_eq!(
        serde_json::from_value::<QueryRequest>(serde_json::to_value(&q).unwrap()).unwrap(),
        q
    );
    assert_eq!(
        CommandRequest::PacketRouteClose(PacketRouteCloseCommand {
            route_id: "x".into(),
            expected_core_instance_id: "y".into(),
            expected_config_revision: None
        })
        .required_permission(),
        Permission::Admin
    );
}

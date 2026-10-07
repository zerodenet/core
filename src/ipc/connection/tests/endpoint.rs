mod statistics;
mod support;

use serde_json::json;
use support::{applied, conditional, state, Fixture, Ipc, PausedReconciler};
use tokio::net::UdpSocket;

#[tokio::test]
async fn ipc_confirms_all_four_endpoint_operations_and_physical_listener_changes() {
    let fixture = Fixture::new().await;
    let mut ipc = Ipc::new(fixture.handle.clone());
    let original = ipc.endpoint().await;
    let params = conditional(
        &original,
        json!({"endpoint_id":"endpoint:ipc-test","directions":{"inbound":false,"outbound":true}}),
    );
    let directions = applied(ipc.command("endpoints.set_directions", params).await);
    assert_eq!(
        directions["effective"],
        json!({"inbound":false,"outbound":true})
    );
    assert_eq!(directions["generation"], original["generation"]);
    let stopped = applied(
        ipc.command(
            "endpoints.set_state",
            conditional(&directions, state(false)),
        )
        .await,
    );
    assert_eq!(stopped["state"], "stopped");
    let released = UdpSocket::bind(("127.0.0.1", fixture.port)).await.unwrap();
    drop(released);
    let started = applied(
        ipc.command("endpoints.set_state", conditional(&stopped, state(true)))
            .await,
    );
    assert_eq!(started["state"], "running");
    let restarted = applied(
        ipc.command(
            "endpoints.restart",
            conditional(&started, json!({"endpoint_id":"endpoint:ipc-test"})),
        )
        .await,
    );
    assert!(restarted["generation"].as_u64().unwrap() > started["generation"].as_u64().unwrap());
    let restored = applied(
        ipc.command(
            "endpoints.clear_overrides",
            conditional(&restarted, json!({"endpoint_id":"endpoint:ipc-test"})),
        )
        .await,
    );
    assert_eq!(restored["state_source"], "config");
    assert_eq!(
        restored["effective"],
        json!({"inbound":true,"outbound":true})
    );
    assert!(UdpSocket::bind(("127.0.0.1", fixture.port)).await.is_err());
    ipc.close().await;
    fixture.running.shutdown().await.unwrap();
}

#[tokio::test]
async fn ipc_endpoint_commands_reject_stale_instance_and_intent_without_mutating_state() {
    let fixture = Fixture::new().await;
    let mut ipc = Ipc::new(fixture.handle.clone());
    let original = ipc.endpoint().await;
    for method in [
        "endpoints.set_state",
        "endpoints.set_directions",
        "endpoints.restart",
        "endpoints.clear_overrides",
    ] {
        let mut params = conditional(&original, json!({"endpoint_id":"endpoint:ipc-test"}));
        if method == "endpoints.set_state" {
            params["enabled"] = false.into();
        }
        if method == "endpoints.set_directions" {
            params["directions"] = json!({"inbound":false,"outbound":true});
        }
        for field in ["expected_core_instance_id", "expected_intent_revision"] {
            let mut stale = params.clone();
            stale[field] = if field == "expected_core_instance_id" {
                json!("obsolete-core")
            } else {
                json!(u64::MAX)
            };
            let response = ipc.command(method, stale).await;
            assert!(!response.ok);
            let error = response.error.unwrap();
            assert_eq!(error.code, "conflict");
            assert_eq!(
                error.field_path.as_deref(),
                Some(format!("params.{field}").as_str())
            );
            let current = ipc.endpoint().await;
            for key in [
                "intent_revision",
                "generation",
                "enabled",
                "allowed",
                "state",
            ] {
                assert_eq!(current[key], original[key]);
            }
        }
    }
    ipc.close().await;
    fixture.running.shutdown().await.unwrap();
}

#[tokio::test]
async fn ipc_endpoint_bind_failure_reports_rollback_and_restores_stopped_intent() {
    let fixture = Fixture::new().await;
    let mut ipc = Ipc::new(fixture.handle.clone());
    let stopped = applied(ipc.command("endpoints.set_state", state(false)).await);
    let occupied = UdpSocket::bind(("127.0.0.1", fixture.port)).await.unwrap();
    let response = ipc
        .command("endpoints.set_state", conditional(&stopped, state(true)))
        .await;
    assert!(!response.ok);
    assert_eq!(response.error.as_ref().unwrap().code, "internal");
    assert!(response
        .error
        .unwrap()
        .message
        .contains("restored previous"));
    let restored = ipc.endpoint().await;
    assert_eq!(restored["intent_revision"], stopped["intent_revision"]);
    assert_eq!(restored["enabled"], false);
    assert_eq!(restored["state"], "stopped");
    assert!(!restored["last_error"].is_null());
    drop(occupied);
    applied(ipc.command("endpoints.set_state", state(true)).await);
    ipc.close().await;
    fixture.running.shutdown().await.unwrap();
}

#[tokio::test]
async fn ipc_endpoint_transaction_survives_lost_acknowledgement_and_can_be_queried() {
    let fixture = Fixture::new().await;
    let reconciler = std::sync::Arc::new(PausedReconciler::default());
    reconciler.arm();
    let controlled = fixture
        .handle
        .clone()
        .with_config_apply_reconciler(reconciler.clone());
    let mut ipc = Ipc::new(controlled);
    ipc.send(json!({"type":"command","method":"endpoints.set_state","params":state(false)}))
        .await;
    reconciler.wait().await;
    ipc.task.abort();
    let _ = (&mut ipc.task).await;
    drop(ipc);
    reconciler.resume.notify_one();
    let mut recovered = Ipc::new(fixture.handle.clone());
    // Idempotent confirmation waits for the detached transaction's lock.
    let stopped = applied(recovered.command("endpoints.set_state", state(false)).await);
    let queried = recovered.endpoint().await;
    assert_eq!(queried["state"], "stopped");
    assert_eq!(queried["intent_revision"], stopped["intent_revision"]);
    recovered.close().await;
    fixture.running.shutdown().await.unwrap();
}

#[tokio::test]
async fn ipc_endpoint_and_statistics_commands_reject_non_admin_before_execution() {
    let fixture = Fixture::new().await;
    let mut query = Ipc::new(fixture.handle.clone());
    let original = query.endpoint().await;
    let traffic = query.traffic().await;
    let read_only = zero_api::AuthContext {
        subject: None,
        permissions: vec![zero_api::Permission::Read, zero_api::Permission::Control],
    };
    let mut denied = Ipc::with_auth(fixture.handle.clone(), read_only);
    for (method, params) in [
        ("endpoints.set_state", state(false)),
        (
            "endpoints.set_directions",
            json!({"endpoint_id":"endpoint:ipc-test","directions":{"inbound":false,"outbound":true}}),
        ),
        (
            "endpoints.restart",
            json!({"endpoint_id":"endpoint:ipc-test"}),
        ),
        (
            "endpoints.clear_overrides",
            json!({"endpoint_id":"endpoint:ipc-test"}),
        ),
        (
            "stats.reset",
            json!({"expected_core_instance_id":"core", "targets":[]}),
        ),
    ] {
        let response = denied.command(method, params).await;
        assert!(!response.ok);
        assert_eq!(response.error.unwrap().code, "permission_denied");
    }
    assert_eq!(
        query.endpoint().await["intent_revision"],
        original["intent_revision"]
    );
    assert_eq!(query.traffic().await["stats_epoch"], traffic["stats_epoch"]);
    denied.close().await;
    query.close().await;
    fixture.running.shutdown().await.unwrap();
}

#[tokio::test]
async fn ipc_live_outbound_contraction_preserves_resource_and_checks_stale_intent() {
    let fixture = Fixture::new().await;
    let mut ipc = Ipc::new(fixture.handle.clone());
    let original = ipc.endpoint().await;
    let directions =
        json!({"endpoint_id":"endpoint:ipc-test","directions":{"inbound":true,"outbound":false}});
    assert_eq!(
        original["supported"]["operation_capabilities"]["set_directions"]
            ["live_direction_contraction"]["outbound"],
        true
    );
    let narrowed = applied(
        ipc.command(
            "endpoints.set_directions",
            conditional(&original, directions.clone()),
        )
        .await,
    );
    assert_eq!(narrowed["state"], "running");
    assert_eq!(narrowed["generation"], original["generation"]);
    assert_eq!(
        narrowed["started_at_unix_ms"],
        original["started_at_unix_ms"]
    );
    assert_eq!(
        narrowed["effective"],
        json!({"inbound":true,"outbound":false})
    );
    let stale = ipc
        .command(
            "endpoints.set_directions",
            conditional(&original, directions),
        )
        .await;
    assert!(!stale.ok);
    assert_eq!(stale.error.unwrap().code, "conflict");
    assert_eq!(
        ipc.endpoint().await["intent_revision"],
        narrowed["intent_revision"]
    );
    let resumed = applied(
        ipc.command(
            "endpoints.set_directions",
            conditional(&narrowed, json!({"endpoint_id":"endpoint:ipc-test","directions":{"inbound":true,"outbound":true}})),
        )
        .await,
    );
    assert_eq!(
        resumed["effective"],
        json!({"inbound":true,"outbound":true})
    );
    assert_eq!(resumed["state"], "running");
    assert_eq!(resumed["generation"], original["generation"]);
    ipc.close().await;
    fixture.running.shutdown().await.unwrap();
}

#[tokio::test]
async fn ipc_queries_and_confirms_a_packet_route_close_without_changing_endpoint_intent() {
    let fixture = Fixture::new().await;
    let engine = fixture.handle.engine_handle().inner().clone();
    let lease = engine
        .register_packet_route(zero_api::PacketRouteSnapshot {
            route_id: String::new(),
            core_instance_id: String::new(),
            config_revision: 0,
            inbound_tag: "endpoint/ipc-test".into(),
            outbound_tag: "ipc-test".into(),
            endpoints: vec![],
            source: "10.0.0.2".into(),
            destination: "10.0.0.3".into(),
            ip_protocol: 17,
            translated: false,
            started_at_unix_ms: 0,
            state: zero_api::PacketRouteState::Active,
        })
        .unwrap();
    let mut ipc = Ipc::new(fixture.handle.clone());
    let before = ipc.endpoint().await;
    let page = ipc
        .query(json!({"packet_routes":{"offset":0,"limit":100}}))
        .await;
    let id = page["packet_routes"]["routes"][0]["route_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let control = lease.control();
    let owner = tokio::spawn(async move {
        control.cancelled().await;
        drop(lease);
    });
    let ack = ipc
        .command(
            "packet_routes.close",
            json!({"route_id":id,"expected_core_instance_id":engine.core_instance_id()}),
        )
        .await;
    assert!(ack.ok, "{:?}", ack.error);
    assert_eq!(ack.result.unwrap()["result"]["closed"], true);
    owner.await.unwrap();
    assert_eq!(
        ipc.endpoint().await["intent_revision"],
        before["intent_revision"]
    );
    assert_eq!(
        ipc.query(json!({"packet_routes":{}})).await["packet_routes"]["total"],
        0
    );
    ipc.close().await;
    fixture.running.shutdown().await.unwrap();
}

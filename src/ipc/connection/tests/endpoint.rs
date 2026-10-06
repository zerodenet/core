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
async fn ipc_outbound_contraction_requires_stop_and_supports_the_existing_client_sequence() {
    let fixture = Fixture::new().await;
    let mut ipc = Ipc::new(fixture.handle.clone());
    let original = ipc.endpoint().await;
    let directions =
        json!({"endpoint_id":"endpoint:ipc-test","directions":{"inbound":true,"outbound":false}});
    let rejected = ipc
        .command(
            "endpoints.set_directions",
            conditional(&original, directions.clone()),
        )
        .await;
    assert!(!rejected.ok);
    assert_eq!(rejected.error.unwrap().code, "unsupported");
    assert_eq!(
        ipc.endpoint().await["intent_revision"],
        original["intent_revision"]
    );
    let stopped = applied(
        ipc.command("endpoints.set_state", conditional(&original, state(false)))
            .await,
    );
    let narrowed = applied(
        ipc.command(
            "endpoints.set_directions",
            conditional(&stopped, directions),
        )
        .await,
    );
    assert_eq!(narrowed["state"], "stopped");
    let resumed = applied(
        ipc.command("endpoints.set_state", conditional(&narrowed, state(true)))
            .await,
    );
    assert_eq!(
        resumed["effective"],
        json!({"inbound":true,"outbound":false})
    );
    assert_eq!(resumed["state"], "running");
    ipc.close().await;
    fixture.running.shutdown().await.unwrap();
}

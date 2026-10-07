use super::support::{Fixture, Ipc, PausedReconciler};
use serde_json::json;
use std::sync::Arc;

#[tokio::test]
async fn ipc_statistics_reset_waits_for_config_and_endpoint_reconciliation() {
    for (method, rebuild) in [
        ("config.apply_runtime", true),
        ("endpoints.restart", true),
        ("config.apply_runtime", false),
    ] {
        let fixture = Fixture::new().await;
        let reconciler = Arc::new(PausedReconciler::default());
        let controlled = fixture
            .handle
            .clone()
            .with_config_apply_reconciler(reconciler.clone());
        let mut operation = Ipc::new(controlled);
        let mut reset = Ipc::new(fixture.handle.clone());
        let mut resource_reset = Ipc::new(fixture.handle.clone());
        let before = reset.traffic().await;
        let scope = json!({"kind":"endpoint","endpoint_id":"endpoint:ipc-test"});
        let resource = resource_reset
            .query(json!({"traffic_stat":{"scope":scope}}))
            .await["traffic_stat"]
            .clone();
        let original_endpoint = reset.endpoint().await;
        let mut candidate = fixture.config.clone();
        if rebuild {
            let replacement = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
            candidate["endpoints"][0]["listen"]["port"] =
                replacement.local_addr().unwrap().port().into();
            drop(replacement);
        } else {
            // A route-only edit retains the physical endpoint components.
            candidate["route"]["final"] = json!({"type":"reject"});
        }
        let params = if method == "config.apply_runtime" {
            json!({"config":candidate})
        } else {
            json!({"endpoint_id":"endpoint:ipc-test"})
        };
        reconciler.arm();
        operation
            .send(json!({"type":"command","method":method,"params":params}))
            .await;
        reconciler.wait().await;
        let params = json!({"expected_core_instance_id":before["core_instance_id"],
            "operation_id":format!("ipc-reset-{method}"),
            "targets":[{"scope":{"kind":"global"},"expected_stats_epoch":before["stats_epoch"]}]});
        reset
            .send(json!({"type":"command","method":"stats.reset","params":params}))
            .await;
        reset.assert_pending().await;
        resource_reset.send(json!({"type":"command","method":"stats.reset","params":{
            "expected_core_instance_id":resource["core_instance_id"],
            "operation_id":format!("ipc-resource-{method}"),
            "targets":[{"scope":scope,"expected_stats_epoch":resource["stats_epoch"],"expected_generation":resource["generation"]}]
        }})).await;
        resource_reset.assert_pending().await;
        reconciler.resume.notify_one();
        let changed = operation.read().await;
        assert!(changed.ok, "{:?}", changed.error);
        let response = reset.read().await;
        assert!(response.ok, "{:?}", response.error);
        let confirmed = response.result.unwrap()["result"]["snapshots"][0].clone();
        assert_ne!(confirmed["stats_epoch"], before["stats_epoch"]);
        let actual = reset.traffic().await;
        assert_eq!(confirmed["stats_epoch"], actual["stats_epoch"]);
        assert_eq!(confirmed["config_revision"], actual["config_revision"]);
        let resource_response = resource_reset.read().await;
        let current = resource_reset
            .query(json!({"traffic_stat":{"scope":scope}}))
            .await["traffic_stat"]
            .clone();
        let endpoint = reset.endpoint().await;
        if rebuild {
            assert!(!resource_response.ok);
            let error = resource_response.error.unwrap();
            assert_eq!(error.code, "conflict");
            assert_eq!(
                error.field_path.as_deref(),
                Some("params.targets[0].expected_generation")
            );
            assert_eq!(current["stats_epoch"], resource["stats_epoch"]);
            assert!(
                current["generation"].as_u64().unwrap() > resource["generation"].as_u64().unwrap()
            );
        } else {
            // Policy edits preserve generation; reset reports the committed
            // revision while opening only the requested statistics period.
            assert!(resource_response.ok, "{:?}", resource_response.error);
            let confirmed = resource_response.result.unwrap()["result"]["snapshots"][0].clone();
            assert_eq!(confirmed["config_revision"], current["config_revision"]);
            assert_eq!(confirmed["generation"], current["generation"]);
            assert_eq!(current["generation"], resource["generation"]);
            assert_eq!(confirmed["stats_epoch"], current["stats_epoch"]);
            assert_ne!(current["stats_epoch"], resource["stats_epoch"]);
        }
        assert_eq!(endpoint["state"], "running");
        if rebuild {
            assert!(
                endpoint["generation"].as_u64().unwrap()
                    > original_endpoint["generation"].as_u64().unwrap()
            );
        } else {
            assert_eq!(endpoint["generation"], original_endpoint["generation"]);
        }
        operation.close().await;
        reset.close().await;
        resource_reset.close().await;
        fixture.running.shutdown().await.unwrap();
    }
}

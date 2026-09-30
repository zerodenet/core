#![cfg(feature = "wireguard")]

#[path = "endpoint_management/support.rs"]
mod management;
mod support;

use management::{config, endpoint, handle, ready, set_state};
use std::sync::Arc;
use support::{free_udp_port, spawn_engine, wait_for};
use tokio::{
    net::UdpSocket,
    sync::Notify,
    time::{timeout, Duration},
};
use zero_api::{
    ApiErrorCode, CommandRequest, CommandService, EndpointGetQuery, EndpointPersistence,
    EndpointRuntimeState, EndpointSetStateCommand, QueryRequest, QueryResponse, QueryService,
};
use zero_config::RuntimeConfig;
use zero_proxy::{ConfigApplyReconciler, ConfigReconcileResult, Proxy};

#[derive(Default)]
struct PausedReconciler {
    entered: Notify,
    resume: Notify,
}

#[async_trait::async_trait]
impl ConfigApplyReconciler for PausedReconciler {
    fn validate(&self, _: &RuntimeConfig, _: &RuntimeConfig) -> Result<(), String> {
        Ok(())
    }
    async fn reconcile(&self, _: Arc<RuntimeConfig>) -> Result<ConfigReconcileResult, String> {
        self.entered.notify_one();
        self.resume.notified().await;
        Ok(ConfigReconcileResult::default())
    }
}

#[tokio::test]
async fn dropping_request_does_not_abandon_staged_endpoint_transaction() {
    let (a, b) = (free_udp_port(), free_udp_port());
    let proxy = Proxy::new(config(a, b)).unwrap();
    let plain = handle(&proxy);
    let reconciler = Arc::new(PausedReconciler::default());
    let controlled = plain
        .clone()
        .with_config_apply_reconciler(reconciler.clone());
    let running = spawn_engine(proxy);
    ready(&plain).await;
    let request =
        tokio::spawn(async move { controlled.execute_acknowledged(set_state("a", false)).await });
    timeout(Duration::from_secs(5), reconciler.entered.notified())
        .await
        .unwrap();
    request.abort();
    reconciler.resume.notify_one();
    // A later command must acquire the same transaction lock. Its completion
    // proves the detached executor released it after finishing reconciliation.
    timeout(
        Duration::from_secs(5),
        plain.execute_acknowledged(set_state("b", false)),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(endpoint(&plain, "a").state, EndpointRuntimeState::Stopped);
    assert_eq!(endpoint(&plain, "b").state, EndpointRuntimeState::Stopped);
    let released = UdpSocket::bind(("127.0.0.1", a)).await.unwrap();
    drop(released);
    plain
        .execute_acknowledged(set_state("a", true))
        .await
        .unwrap();
    assert_eq!(endpoint(&plain, "a").state, EndpointRuntimeState::Running);
    running.shutdown().await.unwrap();
}

#[tokio::test]
async fn explicitly_linked_legacy_roles_share_runtime_control_but_require_canonical_persistence() {
    let (a, b) = (free_udp_port(), free_udp_port());
    let mut legacy = config(a, b);
    legacy.endpoints.clear();
    let proxy = Proxy::new(legacy).unwrap();
    let handle = handle(&proxy);
    let running = spawn_engine(proxy);
    let id = "legacy:inbound:endpoint/a";
    let observe = || {
        let QueryResponse::Endpoint(snapshot) = handle
            .query(QueryRequest::Endpoint(EndpointGetQuery {
                endpoint_id: id.into(),
            }))
            .unwrap()
        else {
            panic!("wrong response")
        };
        snapshot
    };
    wait_for("legacy endpoint running", || {
        observe().state == EndpointRuntimeState::Running
    })
    .await;
    let command = |enabled, persistence| {
        CommandRequest::EndpointSetState(EndpointSetStateCommand {
            endpoint_id: id.into(),
            enabled,
            persistence,
            expected_intent_revision: None,
            expected_core_instance_id: None,
        })
    };
    handle
        .execute_acknowledged(command(false, EndpointPersistence::RuntimeOnly))
        .await
        .unwrap();
    let stopped = observe();
    assert_eq!(stopped.state, EndpointRuntimeState::Stopped);
    assert_eq!(stopped.inbound_tags, ["endpoint/a"]);
    assert_eq!(stopped.outbound_tags, ["a"]);
    let released = UdpSocket::bind(("127.0.0.1", a)).await.unwrap();
    assert_eq!(
        handle
            .execute_acknowledged(command(true, EndpointPersistence::SourceFile))
            .await
            .unwrap_err()
            .code,
        ApiErrorCode::Unsupported
    );
    drop(released);
    handle
        .execute_acknowledged(command(true, EndpointPersistence::RuntimeOnly))
        .await
        .unwrap();
    assert_eq!(observe().state, EndpointRuntimeState::Running);
    running.shutdown().await.unwrap();
}

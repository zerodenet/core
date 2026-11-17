//! The real application prepare hook retires its original network Epoch first.
use super::super::*;
use crate::application::services::{ApplicationServices, ApplicationStartupSlot};
use zero_config::RuntimeConfig;
use zero_engine::Engine;
use zero_proxy::{ConfigApplyReconciler, Proxy};

#[tokio::test]
async fn shared_task_limit_change_retires_event_native_and_scope_before_host_installation() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let directory = tempfile::tempdir().unwrap();
        let config = RuntimeConfig::parse(
            &serde_json::json!({
                "runtime":{"component_total_max_tasks":64},
                "api":{"event_sinks":[{"type":"jsonl","tag":"audit",
                    "path":directory.path().join("events.jsonl"),"events":["engine.warning"]}]},
                "route":{"final":{"type":"direct"}}
            })
            .to_string(),
        )
        .unwrap();
        let engine = Engine::new(config.clone()).unwrap();
        let proxy = Proxy::from_engine(engine.clone()).unwrap();
        let running = proxy.spawn();
        let (instance, originals) = proxy.application_network_originals().unwrap();
        let budget = proxy.application_task_budget();
        let network = Arc::new(ApplicationEventSinkTcpDialer::new(
            proxy.egress_interface_control(),
            instance,
            originals,
            budget.clone(),
        ));
        let startup = ApplicationStartupSlot::new();
        let services = ApplicationServices::start_with_network_owned(
            engine,
            None,
            zero_connector::EventDispatcherNetwork::new(network.clone()),
            &startup,
            Some((
                proxy.application_control_budget(),
                proxy.embedded_command_owner(),
            )),
            None,
        )
        .await
        .unwrap();
        let original = network.current().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer = listener.local_addr().unwrap();
        let deadline = Instant::now() + Duration::from_secs(3);
        let egress = proxy.egress_interface_control();
        let stream = tokio::select! {
            biased;
            result = original.connect(peer, &egress, deadline) => result.unwrap(),
            _ = original.drive() => panic!("actual original Scope unexpectedly ended"),
        };
        let (peer, _) = listener.accept().await.unwrap();
        drop(stream); // Return only the transferred view, not a native close receipt.
        assert!(!original.closed());
        let mut target = config.clone();
        target.runtime.component_total_max_tasks = Some(128);
        let prepared = services
            .prepare_carrier_budget(&config, Arc::new(target))
            .await
            .unwrap();
        assert!(
            prepared.is_some(),
            "shared task-root changes must prepare the real application owner"
        );
        assert!(
            original.closed(),
            "native close, original Scope join and ACK precede Host installation"
        );
        assert_eq!(
            budget.usage().limit,
            64,
            "prepare grants no replacement root policy"
        );
        drop(peer);
        services.shutdown_status_monitor().await.unwrap();
        services.shutdown_dispatcher().await.unwrap();
        running.shutdown().await.unwrap();
        assert_eq!(budget.usage().charged, 0);
    })
    .await
    .unwrap();
}

use super::*;
use tokio::{sync::watch, task::JoinHandle};
pub(super) struct Server {
    upstream: Proxy,
    shutdown: watch::Sender<bool>,
    worker: JoinHandle<Result<(), zero_engine::EngineError>>,
    pub fixture: Fixture,
    _material: tempfile::TempDir,
}
impl Server {
    pub async fn start(protocol: &str) -> Self {
        let material = material();
        let mut inbound = inbound_credentials(protocol);
        inbound["tls"] = json!({"cert_path":"ca.pem", "key_path":"key.pem"});
        inbound["ws"] = json!({"path":"/mux"});
        let config = config_at(
            json!({"inbounds":[{"tag":"incoming",
            "listen":{"address":"127.0.0.1","port":1},"protocol":inbound}],
            "route":{"final":{"type":"direct"}}}),
            material.path(),
        );
        let upstream = Proxy::new(config.clone()).unwrap();
        start_components(&upstream).await;
        let mut input = config.inbounds[0].clone();
        input.listen.port = 0;
        // Keep the actual bound listener until the real runtime takes it.
        let bound = upstream
            .protocols
            .bind_inbound(&input, config.source_dir())
            .await
            .unwrap();
        let address = match &bound {
            crate::protocol_registry::BoundInbound::Tcp(listener) => listener.local_addr().unwrap(),
            _ => panic!("actual TCP listener"),
        };
        let mut operation = upstream
            .protocols
            .prepare_inbound_listener(input, config.source_dir())
            .unwrap();
        let (shutdown, rx) = watch::channel(false);
        let installed = upstream
            .inbound_owners
            .install_listener(
                "incoming",
                &upstream.protocols,
                operation.internal_tasks(),
                shutdown.clone(),
            )
            .unwrap();
        operation.install_resources(&installed).unwrap();
        let owner = installed.owner();
        let runtime = crate::runtime::route_runtime::InboundListenerRuntime::new(
            crate::runtime::route_runtime::SharedIngressRuntimeServices::new(
                upstream.tcp_runtime_services(),
            ),
            "incoming".into(),
            Some(owner.task_access()),
        );
        owner
            .spawn_driver(operation.execute(runtime, bound, rx))
            .unwrap();
        owner.seal_listener_installation();
        let inbound_worker = tokio::spawn(async move { owner.observe_listener().await });

        Self {
            upstream,
            shutdown,
            worker: inbound_worker,
            fixture: Fixture::start(address).await,
            _material: material,
        }
    }
    pub async fn close(self) {
        self.shutdown.send(true).unwrap();
        self.worker.await.unwrap().unwrap();
        self.upstream
            .inbound_owners
            .shutdown("incoming", deadline())
            .await
            .unwrap();
        self.fixture.finish().await;
        self.upstream
            .protocols
            .shutdown_components(deadline())
            .await
            .unwrap();
    }
}

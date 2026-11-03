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
        let listener = crate::runtime::test_listener::start(&upstream, input).await;
        let address = listener.address;
        let shutdown = listener.stop;
        let inbound_worker = listener.worker;

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

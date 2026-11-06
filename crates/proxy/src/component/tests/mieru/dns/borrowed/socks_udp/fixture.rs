//! The remote SOCKS endpoint uses its real standard source and Datagram driver.
use super::*;
pub(super) struct Prefix {
    pub(super) node: zero_config::OutboundConfig,
    proxy: Proxy,
    stop: tokio::sync::watch::Sender<bool>,
    task: tokio::task::JoinHandle<Result<(), zero_engine::EngineError>>,
}
impl Prefix {
    pub(super) async fn new() -> Self {
        let config=zero_config::RuntimeConfig::parse(r#"{"inbounds":[{"tag":"incoming","listen":{"address":"127.0.0.1","port":1},"protocol":{"type":"socks5"}}],"route":{"final":{"type":"direct"}}}"#).unwrap();
        let proxy = Proxy::new(config.clone()).unwrap();
        let startup = proxy.engine().runtime_snapshot().config().clone();
        proxy
            .protocols
            .start_components(startup.clone(), deadline())
            .await
            .unwrap();
        proxy
            .protocols
            .commit_components(&startup, || Ok(()))
            .unwrap();
        let mut inbound = config.inbounds[0].clone();
        inbound.listen.port = 0;
        let listener = crate::runtime::test_listener::start(&proxy, inbound).await;
        let node=serde_json::from_value(serde_json::json!({"tag":"socks","protocol":{"type":"socks5","server":"127.0.0.1","port":listener.address.port()}})).unwrap();
        Self {
            node,
            proxy,
            stop: listener.stop,
            task: listener.worker,
        }
    }
    pub(super) async fn close(self) {
        self.stop.send_replace(true);
        self.proxy
            .inbound_owners
            .shutdown("incoming", deadline())
            .await
            .unwrap();
        tokio::time::timeout_at(deadline(), self.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        self.proxy
            .protocols
            .shutdown_components(deadline())
            .await
            .unwrap();
    }
}

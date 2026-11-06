//! The remote SOCKS endpoint uses one ordinary running Source and its receipts.
use super::*;
pub(super) struct Prefix {
    pub(super) node: zero_config::OutboundConfig,
    running: crate::RunningProxy,
    original: Arc<crate::runtime::inbound_owners::InboundOwner>,
}
impl Prefix {
    pub(super) async fn new() -> Self {
        let config=zero_config::RuntimeConfig::parse(r#"{"inbounds":[{"tag":"incoming","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}],"route":{"final":{"type":"direct"}}}"#).unwrap();
        let proxy = Proxy::new(config).unwrap();
        let registry = proxy.system_tun_source_registry();
        let running = proxy.spawn();
        let original = tokio::time::timeout_at(deadline(), async {
            let owner = loop {
                if let Some(owner) = registry.owner("incoming") {
                    break owner;
                }
                tokio::task::yield_now().await;
            };
            owner.wait_ready().await.unwrap();
            owner
        })
        .await
        .unwrap();
        let address = *original
            .original_listener_address
            .get()
            .expect("same actual ordinary SOCKS Source published its bound listener");
        let node=serde_json::from_value(serde_json::json!({"tag":"socks","protocol":{"type":"socks5","server":"127.0.0.1","port":address.port()}})).unwrap();
        Self {
            node,
            running,
            original,
        }
    }
    pub(super) async fn close(self) {
        self.running.shutdown().await.unwrap();
        let terminal = self
            .original
            .terminal_for_test()
            .expect("same original ordinary SOCKS Source is sealed and terminal");
        assert!(terminal.failure().is_none(), "{:?}", terminal.failure());
        assert!(
            terminal.joined_tasks() > 0,
            "same Source Scope consumed its actual joins"
        );
        assert!(
            terminal.closed_io_receipts() >= 2,
            "same original TCP and UDP listener native receipts"
        );
    }
}

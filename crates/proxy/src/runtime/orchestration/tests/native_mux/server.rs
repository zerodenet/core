use super::*;
pub(super) struct Server {
    running: crate::RunningProxy,
    source: std::sync::Arc<crate::runtime::inbound_owners::InboundOwner>,
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
            "listen":{"address":"127.0.0.1","port":0},"protocol":inbound}],
            "route":{"final":{"type":"direct"}}}),
            material.path(),
        );
        let upstream = Proxy::new(config.clone()).unwrap();
        let registry = upstream.system_tun_source_registry();
        let running = start_components(&upstream).await;
        let source = timeout(Duration::from_secs(5), async {
            loop {
                if let Some(source) = registry.owner("incoming") {
                    break source;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        source.wait_ready().await.unwrap();
        let address = *source
            .original_listener_address
            .get()
            .expect("same actual Source bound address");
        Self {
            running,
            source,
            fixture: Fixture::start(address).await,
            _material: material,
        }
    }
    pub async fn close(self) {
        self.running.shutdown().await.unwrap();
        let closed = self
            .source
            .terminal_for_test()
            .expect("same actual Source native/users/Scope closure");
        assert_eq!(closed.identity(), self.source.identity());
        assert!(closed.failure().is_none());
        assert!(closed.task_failures().is_empty());
        assert!(closed.native_bound_failures().is_empty());
        self.fixture.finish().await;
    }
}

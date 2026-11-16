use crate::{Proxy, ProxyHandle};
use std::{net::SocketAddr, path::Path, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};
use zero_platform_tokio::controlled_carrier::{CarrierOriginObservation, CarrierOwnerRegistration};
const ID: &str = "11111111-2222-3333-4444-555555555555";
pub(super) struct Running {
    pub(super) proxy: Proxy,
    pub(super) address: SocketAddr,
    running: crate::RunningProxy,
    original: Arc<crate::runtime::inbound_owners::InboundOwner>,
}
impl Running {
    pub(super) fn handle(&self) -> ProxyHandle {
        ProxyHandle::new(
            zero_engine::EngineHandle::new(self.proxy.engine().clone()),
            self.proxy.clone(),
        )
    }
    pub(super) async fn close(self) {
        self.running.shutdown().await.unwrap();
        let closed = self
            .original
            .terminal_for_test()
            .expect("actual original Source native/users/Scope complete");
        assert_eq!(closed.identity(), self.original.identity());
        assert!(closed.failure().is_none());
        assert!(closed.task_failures().is_empty());
        assert!(closed.native_bound_failures().is_empty());
    }
}
pub(super) async fn start(document: serde_json::Value, tag: &str) -> Running {
    start_inner(document, tag, false).await
}
pub(super) async fn start_owned(document: serde_json::Value, tag: &str) -> Running {
    start_inner(document, tag, true).await
}
async fn start_inner(document: serde_json::Value, tag: &str, native: bool) -> Running {
    static TRACE: std::sync::Once = std::sync::Once::new();
    TRACE.call_once(|| {
        // This linked transport is not built with cfg(test). Keep its actual
        // restore census visible without logging any profile or credentials.
        let _ = tracing_subscriber::fmt()
            .with_env_filter("off,zero_transport::pooled_driver=debug")
            .with_test_writer()
            .try_init();
    });
    let config = zero_config::RuntimeConfig::parse(&document.to_string()).unwrap();
    let proxy = if native {
        Proxy::from_engine_with_background(zero_engine::Engine::new(config.clone()).unwrap(), true)
            .unwrap()
    } else {
        Proxy::new(config.clone()).unwrap()
    };
    let registry = proxy.system_tun_source_registry();
    let running = proxy.spawn();
    let original = timeout(Duration::from_secs(5), async {
        loop {
            if let Some(original) = registry.owner(tag) {
                break original;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    original.wait_ready().await.unwrap();
    let handle = ProxyHandle::new(
        zero_engine::EngineHandle::new(proxy.engine().clone()),
        proxy.clone(),
    );
    handle
        .apply_runtime_config_and_wait(config, Duration::from_secs(5))
        .await
        .unwrap();
    let address = *original.original_listener_address.get().unwrap();
    Running {
        proxy,
        address,
        running,
        original,
    }
}

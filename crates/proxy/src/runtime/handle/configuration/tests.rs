use std::sync::{Arc, Mutex};
use std::time::Duration;

use zero_config::RuntimeConfig;
use zero_engine::EngineHandle;
use zero_traits::{DnsResolver, IpAddress};

use super::{ConfigApplyReconciler, ConfigReconcileResult, ProxyHandle};
use crate::runtime::Proxy;

struct RejectChangedFakeIpPool {
    calls: Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl ConfigApplyReconciler for RejectChangedFakeIpPool {
    fn validate(&self, _current: &RuntimeConfig, _candidate: &RuntimeConfig) -> Result<(), String> {
        Ok(())
    }

    async fn reconcile(&self, target: Arc<RuntimeConfig>) -> Result<ConfigReconcileResult, String> {
        let cidr = target
            .runtime
            .dns
            .as_ref()
            .and_then(|dns| dns.fake_ip())
            .expect("Fake-IP config")
            .cidr
            .to_owned();
        self.calls.lock().expect("calls lock").push(cidr.clone());
        if cidr == "198.19.0.0/24" {
            return Err("injected application reconcile failure".to_owned());
        }
        Ok(ConfigReconcileResult::default())
    }
}

fn config(cidr: &str) -> RuntimeConfig {
    RuntimeConfig::parse(&format!(
        r#"{{
            "runtime": {{
                "dns": {{
                    "servers": {{ "system": {{ "type": "system" }} }},
                    "default_server": "system",
                    "answer": {{
                        "type": "fake_ip",
                        "cidr": "{cidr}",
                        "ttl_seconds": 3600,
                        "max_entries": 16
                    }}
                }}
            }},
            "inbounds": [{{
                "tag": "reload-test",
                "listen": {{ "address": "127.0.0.1", "port": 0 }},
                "protocol": {{ "type": "direct" }}
            }}],
            "route": {{ "rules": [], "final": {{ "type": "direct" }} }}
        }}"#
    ))
    .expect("parse config")
}

#[tokio::test]
async fn application_reconcile_failure_keeps_committed_fake_ip_mapping() {
    let original = config("198.18.0.0/24");
    let proxy = Proxy::new(original).expect("build proxy");
    let resolver = proxy.resolver.clone();
    assert_eq!(
        DnsResolver::resolve(resolver.as_ref(), "cached.example")
            .await
            .expect("allocate committed mapping"),
        vec![IpAddress::V4([198, 18, 0, 1])]
    );
    let reconciler = Arc::new(RejectChangedFakeIpPool {
        calls: Mutex::new(Vec::new()),
    });
    let handle = ProxyHandle::new(EngineHandle::new(proxy.engine().clone()), proxy.clone())
        .with_config_apply_reconciler(reconciler.clone());
    let running = proxy.spawn();

    let error = handle
        .apply_config_and_wait(config("198.19.0.0/24"), Duration::from_secs(5))
        .await
        .expect_err("application failure must reject candidate");

    assert!(
        error.contains("restored last-known-good configuration"),
        "{error}"
    );
    assert_eq!(
        resolver
            .lookup_fake_ip(&IpAddress::V4([198, 18, 0, 1]))
            .await
            .as_deref(),
        Some("cached.example")
    );
    assert_eq!(
        DnsResolver::resolve(resolver.as_ref(), "cached.example")
            .await
            .expect("resolve through restored allocator"),
        vec![IpAddress::V4([198, 18, 0, 1])]
    );
    assert_eq!(
        *reconciler.calls.lock().expect("calls lock"),
        vec!["198.19.0.0/24".to_owned(), "198.18.0.0/24".to_owned()]
    );

    running.shutdown().await.expect("shutdown proxy");
}

#[tokio::test]
async fn successful_apply_commits_prepared_fake_ip_pool() {
    let proxy = Proxy::new(config("198.18.0.0/24")).expect("build proxy");
    let resolver = proxy.resolver.clone();
    DnsResolver::resolve(resolver.as_ref(), "old.example")
        .await
        .expect("allocate old mapping");
    let handle = ProxyHandle::new(EngineHandle::new(proxy.engine().clone()), proxy.clone());
    let running = proxy.spawn();

    handle
        .apply_config_and_wait(config("198.19.0.0/24"), Duration::from_secs(5))
        .await
        .expect("commit config");

    assert!(resolver
        .lookup_fake_ip(&IpAddress::V4([198, 18, 0, 1]))
        .await
        .is_none());
    assert_eq!(
        DnsResolver::resolve(resolver.as_ref(), "new.example")
            .await
            .expect("allocate committed replacement"),
        vec![IpAddress::V4([198, 19, 0, 1])]
    );

    running.shutdown().await.expect("shutdown proxy");
}

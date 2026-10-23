use super::*;
use tokio::sync::Notify;

struct Paused {
    entered: Notify,
    release: Notify,
    reject: bool,
    calls: Mutex<Vec<String>>,
}
#[async_trait::async_trait]
impl ConfigApplyReconciler for Paused {
    fn validate(&self, _: &RuntimeConfig, _: &RuntimeConfig) -> Result<(), String> {
        Ok(())
    }
    async fn reconcile(&self, target: Arc<RuntimeConfig>) -> Result<ConfigReconcileResult, String> {
        let cidr = target
            .runtime
            .dns
            .as_ref()
            .unwrap()
            .fake_ip()
            .unwrap()
            .cidr
            .to_owned();
        self.calls.lock().unwrap().push(cidr.clone());
        if cidr == "198.19.0.0/24" {
            self.entered.notify_one();
            self.release.notified().await;
            if self.reject {
                return Err("injected application failure after waiter cancellation".to_owned());
            }
        }
        Ok(ConfigReconcileResult::default())
    }
}
fn paused(reject: bool) -> Arc<Paused> {
    Arc::new(Paused {
        entered: Notify::new(),
        release: Notify::new(),
        reject,
        calls: Mutex::new(vec![]),
    })
}

#[tokio::test]
async fn cancelled_config_waiter_does_not_abandon_actual_dns_commit() {
    let proxy = Proxy::new(config("198.18.0.0/24")).unwrap();
    let resolver = proxy.resolver.clone();
    DnsResolver::resolve(resolver.as_ref(), "old.example")
        .await
        .unwrap();
    let reconciler = paused(false);
    let handle = ProxyHandle::new(EngineHandle::new(proxy.engine().clone()), proxy.clone())
        .with_config_apply_reconciler(reconciler.clone());
    let running = proxy.spawn();
    let before = proxy.engine().config_revision();
    let request = tokio::spawn(async move {
        handle
            .apply_runtime_config_and_wait(config("198.19.0.0/24"), Duration::from_secs(5))
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), reconciler.entered.notified())
        .await
        .unwrap();
    assert!(!request.is_finished());
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    reconciler.release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), proxy.apply_owner.wait_idle())
        .await
        .unwrap()
        .unwrap();
    assert!(proxy.engine().config_revision() > before);
    assert!(resolver
        .lookup_fake_ip(&IpAddress::V4([198, 18, 0, 1]))
        .await
        .is_none());
    assert_eq!(
        DnsResolver::resolve(resolver.as_ref(), "new.example")
            .await
            .unwrap(),
        vec![IpAddress::V4([198, 19, 0, 1])]
    );
    running.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancelled_config_waiter_does_not_abandon_actual_application_and_dns_rollback() {
    let proxy = Proxy::new(config("198.18.0.0/24")).unwrap();
    let resolver = proxy.resolver.clone();
    DnsResolver::resolve(resolver.as_ref(), "old.example")
        .await
        .unwrap();
    let reconciler = paused(true);
    let handle = ProxyHandle::new(EngineHandle::new(proxy.engine().clone()), proxy.clone())
        .with_config_apply_reconciler(reconciler.clone());
    let running = proxy.spawn();
    let before = proxy.engine().config_revision();
    let request = tokio::spawn(async move {
        handle
            .apply_runtime_config_and_wait(config("198.19.0.0/24"), Duration::from_secs(5))
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), reconciler.entered.notified())
        .await
        .unwrap();
    assert!(!request.is_finished());
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    reconciler.release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), proxy.apply_owner.wait_idle())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(proxy.engine().config_revision(), before);
    assert_eq!(
        resolver
            .lookup_fake_ip(&IpAddress::V4([198, 18, 0, 1]))
            .await
            .as_deref(),
        Some("old.example")
    );
    assert_eq!(
        *reconciler.calls.lock().unwrap(),
        vec!["198.19.0.0/24", "198.18.0.0/24"]
    );
    running.shutdown().await.unwrap();
}

#[tokio::test]
async fn host_stop_drives_the_accepted_transaction_rollback_and_joins_before_success() {
    let proxy = Proxy::new(config("198.18.0.0/24")).unwrap();
    DnsResolver::resolve(proxy.resolver.as_ref(), "last-good.example")
        .await
        .unwrap();
    let previous = proxy.engine().runtime_snapshot();
    let reconciler = paused(false);
    let handle = ProxyHandle::new(EngineHandle::new(proxy.engine().clone()), proxy.clone())
        .with_config_apply_reconciler(reconciler.clone());
    let running = proxy.spawn();
    let before = proxy.engine().config_revision();
    let request = tokio::spawn(async move {
        handle
            .apply_runtime_config_and_wait(config("198.19.0.0/24"), Duration::from_secs(5))
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), reconciler.entered.notified())
        .await
        .unwrap();
    let stopped = tokio::spawn(async move { running.shutdown().await });
    tokio::time::timeout(Duration::from_secs(3), async {
        while !proxy.apply_owner.is_closing() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    reconciler.release.notify_one();
    assert!(request
        .await
        .unwrap()
        .unwrap_err()
        .contains("stopped before configuration commit"));
    stopped.await.unwrap().unwrap();
    assert_eq!(proxy.engine().config_revision(), before);
    assert!(Arc::ptr_eq(&proxy.engine().runtime_snapshot(), &previous));
    assert_eq!(
        proxy
            .resolver
            .lookup_fake_ip(&IpAddress::V4([198, 18, 0, 1]))
            .await
            .as_deref(),
        Some("last-good.example")
    );
    assert!(proxy.apply_owner.is_closing());
    assert_eq!(
        *reconciler.calls.lock().unwrap(),
        vec!["198.19.0.0/24", "198.18.0.0/24"]
    );
}

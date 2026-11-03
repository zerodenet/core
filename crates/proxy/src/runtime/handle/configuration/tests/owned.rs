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
    tokio::time::timeout(Duration::from_secs(5), proxy.host.apply().wait_idle())
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
    tokio::time::timeout(Duration::from_secs(5), proxy.host.apply().wait_idle())
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
        while !proxy.host.apply().is_closing() {
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
    assert!(proxy.host.apply().is_closing());
    assert_eq!(
        *reconciler.calls.lock().unwrap(),
        vec!["198.19.0.0/24", "198.18.0.0/24"]
    );
}

#[tokio::test]
async fn persistent_candidate_keeps_actual_last_good_source_until_application_ready() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("zero.json");
    let previous = config("198.18.0.0/24");
    let original = serde_json::to_vec_pretty(&previous).unwrap();
    std::fs::write(&path, &original).unwrap();
    let engine = zero_engine::Engine::new_with_config_path(previous, &path).unwrap();
    let proxy = Proxy::from_engine(engine).unwrap();
    let reconciler = paused(false);
    let handle = ProxyHandle::new(EngineHandle::new(proxy.engine().clone()), proxy.clone())
        .with_config_apply_reconciler(reconciler.clone());
    let running = proxy.spawn();
    let revision = proxy.engine().config_revision();
    let request = tokio::spawn(async move {
        handle
            .apply_config_and_wait(config("198.19.0.0/24"), Duration::from_secs(5))
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), reconciler.entered.notified())
        .await
        .unwrap();
    assert!(!request.is_finished());
    assert_eq!(std::fs::read(&path).unwrap(), original);
    assert_eq!(proxy.engine().config_source_write_generation(), 0);
    assert_eq!(proxy.engine().config_revision(), revision);
    reconciler.release.notify_one();
    request.await.unwrap().unwrap();
    assert_eq!(proxy.engine().config_revision(), revision + 1);
    assert_eq!(proxy.engine().config_source_write_generation(), 1);
    let persisted = zero_config::RuntimeConfig::load_from_path(&path).unwrap();
    assert_eq!(
        persisted
            .runtime
            .dns
            .as_ref()
            .unwrap()
            .fake_ip()
            .unwrap()
            .cidr,
        "198.19.0.0/24"
    );
    zero_runtime::configuration::ConfigurationSource::verify_recovery(&path).unwrap();
    running.shutdown().await.unwrap();
}

#[tokio::test]
async fn application_failure_restores_snapshot_without_ever_overwriting_deployment_source() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("zero.json");
    let previous = config("198.18.0.0/24");
    let original = serde_json::to_vec_pretty(&previous).unwrap();
    std::fs::write(&path, &original).unwrap();
    let engine = zero_engine::Engine::new_with_config_path(previous, &path).unwrap();
    let proxy = Proxy::from_engine(engine).unwrap();
    let snapshot = proxy.engine().runtime_snapshot();
    let reconciler = paused(true);
    let handle = ProxyHandle::new(EngineHandle::new(proxy.engine().clone()), proxy.clone())
        .with_config_apply_reconciler(reconciler.clone());
    let running = proxy.spawn();
    let request = tokio::spawn(async move {
        handle
            .apply_config_and_wait(config("198.19.0.0/24"), Duration::from_secs(5))
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), reconciler.entered.notified())
        .await
        .unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), original);
    reconciler.release.notify_one();
    assert!(request
        .await
        .unwrap()
        .unwrap_err()
        .contains("application service reconciliation failed"));
    assert!(Arc::ptr_eq(&proxy.engine().runtime_snapshot(), &snapshot));
    assert_eq!(std::fs::read(&path).unwrap(), original);
    assert_eq!(proxy.engine().config_source_write_generation(), 0);
    running.shutdown().await.unwrap();
}

#[tokio::test]
async fn waiter_timeout_retains_same_accepted_transaction_until_real_final_ack() {
    let proxy = Proxy::new(config("198.18.0.0/24")).unwrap();
    let reconciler = paused(false);
    let handle = ProxyHandle::new(EngineHandle::new(proxy.engine().clone()), proxy.clone())
        .with_config_apply_reconciler(reconciler.clone());
    let running = proxy.spawn();
    let before = proxy.engine().config_revision();
    let request = tokio::spawn(async move {
        handle
            .apply_runtime_config_and_wait(config("198.19.0.0/24"), Duration::from_millis(250))
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), reconciler.entered.notified())
        .await
        .unwrap();
    assert!(request
        .await
        .unwrap()
        .unwrap_err()
        .contains("configuration wait timed out"));
    assert_eq!(proxy.engine().config_revision(), before);
    let mut idle = Box::pin(proxy.host.apply().wait_idle());
    std::future::poll_fn(|context| {
        use std::future::Future;
        assert!(idle.as_mut().poll(context).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    reconciler.release.notify_one();
    idle.await.unwrap();
    assert_eq!(proxy.engine().config_revision(), before + 1);
    assert_eq!(*reconciler.calls.lock().unwrap(), vec!["198.19.0.0/24"]);
    running.shutdown().await.unwrap();
}

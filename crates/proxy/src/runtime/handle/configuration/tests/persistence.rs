use super::owned::paused;
use super::*;

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
    let committed = proxy.engine().runtime_snapshot();
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
    assert!(Arc::ptr_eq(&proxy.engine().runtime_snapshot(), &committed));
    assert_eq!(
        proxy
            .engine()
            .config()
            .runtime
            .dns
            .as_ref()
            .unwrap()
            .fake_ip()
            .unwrap()
            .cidr,
        "198.18.0.0/24"
    );
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

#[cfg(unix)]
#[tokio::test]
async fn actual_source_write_failure_restores_original_snapshot_dns_and_native_owner() {
    use std::os::unix::fs::PermissionsExt;
    struct RestorePermissions(std::path::PathBuf, std::fs::Permissions);
    impl Drop for RestorePermissions {
        fn drop(&mut self) {
            let _ = std::fs::set_permissions(&self.0, self.1.clone());
        }
    }
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("zero.json");
    let original = config("198.18.0.0/24");
    let bytes = serde_json::to_vec_pretty(&original).unwrap();
    std::fs::write(&path, &bytes).unwrap();
    let engine = zero_engine::Engine::new_with_config_path(original, &path).unwrap();
    let proxy = Proxy::from_engine(engine).unwrap();
    let previous = proxy.engine().runtime_snapshot();
    let handle = ProxyHandle::new(EngineHandle::new(proxy.engine().clone()), proxy.clone());
    let running = proxy.spawn();
    let permissions = std::fs::metadata(directory.path()).unwrap().permissions();
    let restore = RestorePermissions(directory.path().to_owned(), permissions);
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o555)).unwrap();
    let error = handle
        .apply_config_and_wait(config("198.19.0.0/24"), Duration::from_secs(5))
        .await
        .unwrap_err();
    drop(restore);
    assert!(
        error.contains("configuration persistence failed"),
        "{error}"
    );
    assert!(
        error.contains("restored last-known-good configuration"),
        "{error}"
    );
    assert_eq!(std::fs::read(path).unwrap(), bytes);
    assert!(Arc::ptr_eq(&proxy.engine().runtime_snapshot(), &previous));
    assert_eq!(
        proxy
            .engine()
            .configuration_source()
            .observation()
            .disposition,
        zero_runtime::configuration::SourceDisposition::Restored
    );
    assert_eq!(
        DnsResolver::resolve(proxy.resolver.as_ref(), "after-source-failure.example")
            .await
            .unwrap(),
        vec![IpAddress::V4([198, 18, 0, 1])]
    );
    let owner = proxy.inbound_owners.owner("reload-test").unwrap();
    running.shutdown().await.unwrap();
    let closed = owner
        .terminal_for_test()
        .expect("actual native owner joined and closed");
    assert_eq!(closed.identity(), owner.identity());
    assert!(closed.closed_io_receipts() > 0);
    assert!(closed.joined_tasks() > 0);
}

#[test]
fn public_from_path_preserves_load_only_scope_with_the_original_initial_document_closed() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("zero.json");
    let original = br#"{"route":{"final":{"type":"direct"}}}"#;
    std::fs::write(&path, original).unwrap();
    let proxy = Proxy::from_path(&path).unwrap();
    assert!(proxy.engine().config_path().is_none());
    assert_eq!(proxy.engine().config().source_dir(), Some(directory.path()));
    proxy
        .engine()
        .configuration_source()
        .confirm_original_io_closed()
        .unwrap();
    proxy
        .engine()
        .reload_config(RuntimeConfig::parse(r#"{"route":{"final":{"type":"reject"}}}"#).unwrap())
        .unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), original);
    assert!(!directory.path().join("zero.json.zero-recovery").exists());
}

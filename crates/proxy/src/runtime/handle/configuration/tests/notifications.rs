use super::*;

#[tokio::test]
async fn direct_host_notifications_drain_cancelled_accepted_apply_before_original_shutdown() {
    let proxy = Proxy::new(config("198.18.0.0/24")).unwrap();
    DnsResolver::resolve(proxy.resolver.as_ref(), "last-good.example")
        .await
        .unwrap();
    let previous = proxy.engine().runtime_snapshot();
    let reconciler = super::owned::paused(false);
    let handle = ProxyHandle::new(EngineHandle::new(proxy.engine().clone()), proxy.clone())
        .with_config_apply_reconciler(reconciler.clone());
    let running = proxy.spawn();
    let request = tokio::spawn(async move {
        handle
            .apply_runtime_config_and_wait(config("198.19.0.0/24"), Duration::from_secs(5))
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), reconciler.entered.notified())
        .await
        .unwrap();
    // The same accepted task is actually paused in app preparation, after its
    // original proxy Ready ACK. Its waiting caller is independently cancelled.
    assert!(!request.is_finished());
    let owner = proxy.inbound_owners.owner("reload-test").unwrap();
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    let stopped = tokio::spawn(async move { running.shutdown().await });
    tokio::time::timeout(Duration::from_secs(3), async {
        while !proxy.host.apply().is_closing() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!stopped.is_finished());
    for _ in 0..64 {
        proxy.engine().notify_configuration_reconcile();
    }
    reconciler.release.notify_one();
    stopped.await.unwrap().unwrap();
    assert!(Arc::ptr_eq(&proxy.engine().runtime_snapshot(), &previous));
    assert_eq!(
        proxy
            .resolver
            .lookup_fake_ip(&IpAddress::V4([198, 18, 0, 1]))
            .await
            .as_deref(),
        Some("last-good.example")
    );
    assert_eq!(
        *reconciler.calls.lock().unwrap(),
        vec!["198.19.0.0/24", "198.18.0.0/24"]
    );
    let terminal = owner
        .terminal_for_test()
        .expect("same original listener must physically close");
    assert_eq!(terminal.identity(), owner.identity());
    assert!(terminal.closed_io_receipts() > 0);
    assert!(terminal.joined_tasks() > 0);
    proxy
        .engine()
        .configuration_source()
        .confirm_original_io_closed()
        .unwrap();
}

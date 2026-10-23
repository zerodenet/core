use super::*;

#[tokio::test]
async fn installed_monitor_identity_is_the_same_weak_scope_without_lock_or_callback() {
    let scope = TaskScope::suspended_hosted(tokio::runtime::Handle::current(), None);
    let registrar = scope.registrar();
    let monitor = TaskMonitor::new(scope);
    let identity = {
        // Access must not attempt the same lock, publish or deliver any callback.
        let _held = monitor.inner.lock().unwrap();
        let identity = monitor.scope_identity();
        assert!(identity.matches(&registrar));
        identity
    };
    assert!(!identity.registration_open());
    monitor.enable_registration().unwrap();
    assert!(
        !identity.registration_open(),
        "zero declared tasks grant no registration"
    );
    monitor.begin_shutdown();
    assert!(monitor.scope_identity().matches(&registrar));
    drop(monitor);
    assert!(
        identity.matches(&registrar),
        "live original registrar still owns metadata"
    );
    assert!(!identity.registration_open());
    drop(registrar);
    assert!(
        !identity.registration_open(),
        "weak identity cannot retain the old owner"
    );
}

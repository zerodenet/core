use super::*;
#[test]
fn lost_or_retiring_owner_never_becomes_cold_without_confirmed_close() {
    for mode in [Mode::Owned(Weak::new()), Mode::Retiring] {
        let registry = PoolControl::default();
        let old = registry.scoped("node".into());
        *old.scope.0.lock().unwrap() = mode;
        let original = Arc::downgrade(&old.scope);
        drop(old);
        let current = registry.scoped("node".into());
        assert!(original.ptr_eq(&Arc::downgrade(&current.scope)));
        assert!(current.managed().is_err());
        assert!(current.installed().is_err());
        assert!(
            current.enter_legacy().is_err(),
            "no failed owner may start an untracked replacement"
        );
    }
}

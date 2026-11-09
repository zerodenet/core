//! The private proof boundary preserves the actual original allocation.
use super::*;
#[tokio::test]
async fn erased_shared_keeps_exact_bootstrap_weak_and_owner_identity_without_another_task() {
    let mut scope = TaskScope::new(Handle::current(), NonZeroUsize::new(1).unwrap());
    let original = OwnedDnsQueryTasks::new(&scope.registrar(), NonZeroUsize::new(1).unwrap());
    let same = original.clone();
    let other = OwnedDnsQueryTasks::new(&scope.registrar(), NonZeroUsize::new(1).unwrap());
    let weak = original.0.downgrade();
    let actual = weak.upgrade().unwrap();
    assert!(same.same_owner(&original));
    assert!(!other.same_owner(&original));
    assert!(same.0.same_arc(&actual));
    assert!(std::ptr::eq(&*original.0, &*same.0));
    original
        .bootstrap(async {
            assert!(BOOTSTRAP_TARGET.with(|target| std::sync::Weak::ptr_eq(target, &weak)));
        })
        .await
        .unwrap();
    drop(actual);
    drop(original);
    assert!(weak.upgrade().is_some(), "same actual strong owner remains");
    drop(same);
    assert!(
        weak.upgrade().is_none(),
        "no hidden wrapper/owner keeps Shared alive"
    );
    drop(other);
    assert!(scope.shutdown(Instant::now()).await.is_complete());
}

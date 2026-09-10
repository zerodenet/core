use super::fixtures::{connect, key};
use mieru::client::ClientPool;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[tokio::test]
async fn different_transport_profiles_do_not_reuse_the_same_carrier() {
    let pool = ClientPool::default();
    let count = Arc::new(AtomicUsize::new(0));
    let first = key("u", "p").with_profile("mtu=1280;seed=1".into());
    let second = key("u", "p").with_profile("mtu=1400;seed=2".into());
    let _a = pool
        .open(first.clone(), || connect(count.clone(), "u", "p"))
        .await
        .unwrap();
    let _b = pool
        .open(second, || connect(count.clone(), "u", "p"))
        .await
        .unwrap();
    let _c = pool
        .open(first, || connect(count.clone(), "u", "p"))
        .await
        .unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 2);
}

use super::fixtures::*;
use mieru::client::{ClientPool, ClientPoolPolicy};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn grows_on_sustained_load_and_prefers_the_least_loaded_carrier() {
    let pool = ClientPool::with_policy(ClientPoolPolicy {
        max_connections_per_identity: 2,
        scale_out_load_percent: 1,
        max_connection_age: Duration::from_secs(60),
    })
    .unwrap();
    let dials = Arc::new(AtomicUsize::new(0));
    let mut streams = Vec::new();

    for _ in 0..4 {
        streams.push(
            pool.open(key("u", "p"), || connect(dials.clone(), "u", "p"))
                .await
                .unwrap(),
        );
    }

    assert_eq!(dials.load(Ordering::SeqCst), 2);
    assert_eq!(pool.snapshot().connections, 2);
    assert_eq!(pool.snapshot().active_streams, 4);

    let mut fifth = pool
        .open(key("u", "p"), || connect(dials.clone(), "u", "p"))
        .await
        .unwrap();
    assert_eq!(dials.load(Ordering::SeqCst), 2);
    fifth.write_all(b"least-loaded").await.unwrap();
    let mut echoed = [0; 12];
    fifth.read_exact(&mut echoed).await.unwrap();
    assert_eq!(&echoed, b"least-loaded");

    drop(fifth);
    drop(streams);
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
    assert_eq!(pool.snapshot().active_streams, 0);
}

#[tokio::test]
async fn retires_aged_carriers_without_interrupting_existing_streams() {
    let pool = ClientPool::with_policy(ClientPoolPolicy {
        max_connections_per_identity: 2,
        scale_out_load_percent: 100,
        max_connection_age: Duration::from_millis(20),
    })
    .unwrap();
    let dials = Arc::new(AtomicUsize::new(0));
    let mut old = pool
        .open(key("u", "p"), || connect(dials.clone(), "u", "p"))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(40)).await;
    let mut fresh = pool
        .open(key("u", "p"), || connect(dials.clone(), "u", "p"))
        .await
        .unwrap();
    assert_eq!(dials.load(Ordering::SeqCst), 2);
    assert_eq!(pool.snapshot().connections, 1);

    old.write_all(b"old").await.unwrap();
    let mut old_echo = [0; 3];
    old.read_exact(&mut old_echo).await.unwrap();
    assert_eq!(&old_echo, b"old");

    fresh.write_all(b"new").await.unwrap();
    let mut fresh_echo = [0; 3];
    fresh.read_exact(&mut fresh_echo).await.unwrap();
    assert_eq!(&fresh_echo, b"new");
}

#[test]
fn rejects_invalid_pool_policies() {
    for policy in [
        ClientPoolPolicy {
            max_connections_per_identity: 0,
            ..ClientPoolPolicy::default()
        },
        ClientPoolPolicy {
            scale_out_load_percent: 0,
            ..ClientPoolPolicy::default()
        },
        ClientPoolPolicy {
            scale_out_load_percent: 101,
            ..ClientPoolPolicy::default()
        },
        ClientPoolPolicy {
            max_connection_age: Duration::ZERO,
            ..ClientPoolPolicy::default()
        },
    ] {
        assert!(ClientPool::with_policy(policy).is_err());
    }
}

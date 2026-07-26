use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use super::backlog::{MuxResponseBacklog, MuxResponseBacklogPolicy, MUX_RESPONSE_QUEUE_CAPACITY};
use super::{
    try_queue_mux_response, VmessInboundMuxWriter, VmessMuxConnectionPool, VmessMuxDownlink,
    VmessMuxIdentity, VmessMuxPoolKey,
};
use crate::VmessCipher;

#[tokio::test]
async fn eviction_through_a_bridge_clone_removes_cached_mux_connections() {
    let pool = VmessMuxConnectionPool::new();
    let bridge_pool = pool.clone();
    let identity = VmessMuxIdentity::from_parts([9; 16], "none".to_owned(), VmessCipher::None);
    let key = VmessMuxPoolKey::from_config_parts(
        "vmess.test".to_owned(),
        443,
        identity,
        None,
        None,
        None,
        None,
        MuxResponseBacklogPolicy::default(),
    )
    .expect("VMess mux pool key");
    let (stream, _peer) = tokio::io::duplex(64);
    let connection = Arc::new(key.clone().into_pool_conn(stream, 4));
    pool.pool
        .lock()
        .expect("VMess mux pool lock")
        .insert(key, connection);

    assert_eq!(pool.pool.lock().expect("VMess mux pool lock").len(), 1);
    bridge_pool.evict_all();
    assert!(pool.pool.lock().expect("VMess mux pool lock").is_empty());
}

#[tokio::test]
async fn mux_activity_refreshes_the_idle_timeout_before_the_carrier_closes() {
    let identity = VmessMuxIdentity::from_parts([10; 16], "none".to_owned(), VmessCipher::None);
    let key = VmessMuxPoolKey::from_config_parts(
        "vmess.test".to_owned(),
        443,
        identity,
        None,
        None,
        None,
        Some(Duration::from_millis(500)),
        MuxResponseBacklogPolicy::default(),
    )
    .expect("VMess mux pool key");
    let (stream, _peer) = tokio::io::duplex(64);
    let connection = Arc::new(key.into_pool_conn(stream, 4));
    let session_id = connection
        .try_reserve_stream_id()
        .expect("reserve first logical stream");

    tokio::time::sleep(Duration::from_millis(50)).await;
    connection.touch_idle();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!connection.closed.load(Ordering::Acquire));

    connection.release_stream(session_id);
    tokio::time::timeout(Duration::from_secs(1), async {
        while !connection.closed.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("idle VMess MUX carrier should close");
    assert!(connection.try_reserve_stream_id().is_none());
}

#[test]
fn different_response_backlog_policies_do_not_share_a_pool_key() {
    let identity = VmessMuxIdentity::from_parts([12; 16], "none".to_owned(), VmessCipher::None);
    let default_key = VmessMuxPoolKey::from_config_parts(
        "vmess.test".to_owned(),
        443,
        identity.clone(),
        None,
        None,
        None,
        None,
        MuxResponseBacklogPolicy::default(),
    )
    .expect("default VMess mux pool key");
    let tuned_key = VmessMuxPoolKey::from_config_parts(
        "vmess.test".to_owned(),
        443,
        identity,
        None,
        None,
        None,
        None,
        MuxResponseBacklogPolicy::from_config(Some(64), Some(2 * 1024 * 1024))
            .expect("valid tuned VMess backlog policy"),
    )
    .expect("tuned VMess mux pool key");

    assert_ne!(default_key, tuned_key);
}

#[test]
fn inbound_writer_uses_configured_frame_limit() {
    let policy = MuxResponseBacklogPolicy::from_config(Some(1), Some(16 * 1024))
        .expect("valid VMess MUX response backlog policy");
    let backlog = MuxResponseBacklog::from_policy(policy);
    let (tx, _responses) = tokio::sync::mpsc::channel(policy.frames());
    let writer = VmessInboundMuxWriter::new(tx, backlog);

    writer.frame(vec![1]).expect("first response frame");
    let error = writer
        .frame(vec![2])
        .expect_err("second response frame must exceed configured capacity");
    assert!(error.to_string().contains("frame limit"));
}

#[test]
fn inbound_writer_uses_configured_byte_limit() {
    let policy = MuxResponseBacklogPolicy::from_config(Some(2), Some(16 * 1024))
        .expect("valid VMess MUX response backlog policy");
    let backlog = MuxResponseBacklog::from_policy(policy);
    let (tx, _responses) = tokio::sync::mpsc::channel(policy.frames());
    let writer = VmessInboundMuxWriter::new(tx, backlog);

    writer
        .frame(vec![0; 16 * 1024])
        .expect("response at configured byte limit");
    let error = writer
        .frame(vec![1])
        .expect_err("response beyond configured byte limit must fail");
    assert!(error.to_string().contains("byte limit"));
}

#[tokio::test]
async fn response_backlog_byte_overflow_is_explicit_and_releases_reserved_bytes() {
    let backlog = MuxResponseBacklog::new(4);
    let (tx, mut rx) = tokio::sync::mpsc::channel(3);

    assert!(try_queue_mux_response(&backlog, &tx, b"full".to_vec(), 4,));
    assert!(!try_queue_mux_response(&backlog, &tx, b"x".to_vec(), 1,));
    assert_eq!(backlog.used(), 4);

    let Some(VmessMuxDownlink::Data(response)) = rx.recv().await else {
        panic!("expected buffered response");
    };
    assert_eq!(response.into_inner(), b"full");
    assert_eq!(backlog.used(), 0);
    assert!(matches!(rx.recv().await, Some(VmessMuxDownlink::Overflow)));
}

#[tokio::test]
async fn slow_consumer_is_cut_off_at_the_frame_limit() {
    let backlog = MuxResponseBacklog::new(1024);
    let (tx, mut rx) = tokio::sync::mpsc::channel(MUX_RESPONSE_QUEUE_CAPACITY + 1);

    for _ in 0..MUX_RESPONSE_QUEUE_CAPACITY {
        assert!(try_queue_mux_response(&backlog, &tx, vec![1], 1,));
    }
    assert!(!try_queue_mux_response(&backlog, &tx, vec![2], 1,));

    for _ in 0..MUX_RESPONSE_QUEUE_CAPACITY {
        assert!(matches!(rx.recv().await, Some(VmessMuxDownlink::Data(_))));
    }
    assert!(matches!(rx.recv().await, Some(VmessMuxDownlink::Overflow)));
}

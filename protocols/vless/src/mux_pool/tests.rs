use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use super::{try_queue_mux_response, MuxConnectionPool, MuxDownlink, MuxIdentity, PoolKey};
use crate::mux::backlog::{
    MuxResponseBacklog, MuxResponseBacklogPolicy, MUX_RESPONSE_QUEUE_CAPACITY,
};
use tokio::io::AsyncWriteExt;
use zero_core::Address;

#[tokio::test]
async fn eviction_through_a_bridge_clone_removes_cached_mux_connections() {
    let pool = MuxConnectionPool::new();
    let bridge_pool = pool.clone();
    let identity = MuxIdentity::from_uuid([7; 16]);
    let key = PoolKey::from_config_parts(
        "vless.test".to_owned(),
        443,
        identity,
        None,
        None,
        None,
        None,
        None,
        MuxResponseBacklogPolicy::default(),
    );
    let (stream, _peer) = tokio::io::duplex(64);
    let connection = Arc::new(key.clone().into_pool_conn(stream, 4));
    pool.pool
        .lock()
        .expect("mux pool lock")
        .insert(key, connection);

    assert_eq!(pool.pool.lock().expect("mux pool lock").len(), 1);
    bridge_pool.evict_all();
    assert!(pool.pool.lock().expect("mux pool lock").is_empty());
}

#[tokio::test]
async fn xudp_frame_uses_separate_metadata_and_data_lengths() {
    let frame = crate::mux::encode_new_udp_data_frame(
        7,
        &Address::Ipv4([127, 0, 0, 1]),
        5353,
        [1, 2, 3, 4, 5, 6, 7, 8],
        b"query",
    )
    .expect("encode XUDP frame");

    assert_eq!(u16::from_be_bytes([frame[0], frame[1]]), 20);
    assert_eq!(u16::from_be_bytes([frame[2], frame[3]]), 7);
    assert_eq!(frame[4], crate::mux::STATUS_NEW);
    assert_eq!(frame[5], crate::mux::OPTION_DATA);
    assert_eq!(frame[6], crate::mux::NETWORK_UDP);
    assert_eq!(u16::from_be_bytes([frame[7], frame[8]]), 5353);
    assert_eq!(frame[9], 0x01);
    assert_eq!(&frame[10..14], &[127, 0, 0, 1]);
    assert_eq!(&frame[14..22], &[1, 2, 3, 4, 5, 6, 7, 8]);
    assert_eq!(u16::from_be_bytes([frame[22], frame[23]]), 5);
    assert_eq!(&frame[24..], b"query");

    let (mut writer, mut reader) = tokio::io::duplex(128);
    writer.write_all(&frame).await.expect("write frame");
    let decoded = crate::mux::read_mux_frame_tokio(&mut reader)
        .await
        .expect("decode frame");
    assert_eq!(decoded.session_id, 7);
    assert_eq!(decoded.status, crate::mux::STATUS_NEW);
    assert_eq!(decoded.global_id, Some([1, 2, 3, 4, 5, 6, 7, 8]));
    assert_eq!(decoded.payload, b"query");
    let target = decoded.target.expect("decoded target");
    assert_eq!(target.network, crate::mux::NETWORK_UDP);
    assert_eq!(target.port, 5353);
    assert_eq!(target.address, Address::Ipv4([127, 0, 0, 1]));
}

#[tokio::test]
async fn mux_activity_refreshes_the_idle_timeout_before_the_carrier_closes() {
    let identity = MuxIdentity::from_uuid([8; 16]);
    let key = PoolKey::from_config_parts(
        "vless.test".to_owned(),
        443,
        identity,
        None,
        None,
        None,
        None,
        Some(Duration::from_millis(500)),
        MuxResponseBacklogPolicy::default(),
    );
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
    .expect("idle VLESS MUX carrier should close");
    assert!(connection.try_reserve_stream_id().is_none());
}

#[test]
fn different_response_backlog_policies_do_not_share_a_pool_key() {
    let identity = MuxIdentity::from_uuid([11; 16]);
    let default_key = PoolKey::from_config_parts(
        "vless.test".to_owned(),
        443,
        identity.clone(),
        None,
        None,
        None,
        None,
        None,
        MuxResponseBacklogPolicy::default(),
    );
    let tuned_key = PoolKey::from_config_parts(
        "vless.test".to_owned(),
        443,
        identity,
        None,
        None,
        None,
        None,
        None,
        MuxResponseBacklogPolicy::from_config(Some(64), Some(2 * 1024 * 1024))
            .expect("valid tuned VLESS backlog policy"),
    );

    assert!(default_key != tuned_key);
}

#[test]
fn different_reality_fingerprints_do_not_share_a_pool_key() {
    let identity = MuxIdentity::from_uuid([12; 16]);
    let chrome = PoolKey::from_config_parts(
        "vless.test".to_owned(),
        443,
        identity.clone(),
        None,
        Some("public-key"),
        Some("example.com"),
        Some("chrome"),
        None,
        MuxResponseBacklogPolicy::default(),
    );
    let firefox = PoolKey::from_config_parts(
        "vless.test".to_owned(),
        443,
        identity,
        None,
        Some("public-key"),
        Some("example.com"),
        Some("firefox"),
        None,
        MuxResponseBacklogPolicy::default(),
    );

    assert!(chrome != firefox);
}

#[tokio::test]
async fn response_backlog_byte_overflow_is_explicit_and_releases_reserved_bytes() {
    let backlog = MuxResponseBacklog::new(4);
    let (tx, mut rx) = tokio::sync::mpsc::channel(3);

    assert!(try_queue_mux_response(&backlog, &tx, b"full".to_vec(), 4,));
    assert!(!try_queue_mux_response(&backlog, &tx, b"x".to_vec(), 1,));
    assert_eq!(backlog.used(), 4);

    let Some(MuxDownlink::Data(response)) = rx.recv().await else {
        panic!("expected buffered response");
    };
    assert_eq!(response.into_inner(), b"full");
    assert_eq!(backlog.used(), 0);
    assert!(matches!(rx.recv().await, Some(MuxDownlink::Overflow)));
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
        assert!(matches!(rx.recv().await, Some(MuxDownlink::Data(_))));
    }
    assert!(matches!(rx.recv().await, Some(MuxDownlink::Overflow)));
}

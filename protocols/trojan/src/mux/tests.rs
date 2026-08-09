use std::sync::Arc;

use tokio::io::AsyncWriteExt;

use super::backlog::{MuxResponseBacklog, MuxResponseBacklogPolicy};
use super::{
    encode_open_stream_with_network, pool_key_from_config, read_frame_from_tokio,
    TrojanInboundMuxWriter, TrojanMuxConnectionPool, MUX_OPTION_DATA, MUX_STATUS_NEW,
};
use zero_core::{Address, Network};

#[tokio::test]
async fn mux_frame_round_trip_preserves_tcp_target_and_payload() {
    let frame = encode_open_stream_with_network(
        7,
        &Address::Domain("example.com".to_owned()),
        443,
        Network::Tcp,
        b"hello",
    )
    .expect("encode Mux.Cool frame");
    let (mut writer, mut reader) = tokio::io::duplex(frame.len());
    writer.write_all(&frame).await.expect("write frame");
    let decoded = read_frame_from_tokio(&mut reader)
        .await
        .expect("read frame");

    assert_eq!(decoded.session_id, 7);
    assert_eq!(decoded.status, MUX_STATUS_NEW);
    assert_eq!(decoded.option, MUX_OPTION_DATA);
    assert_eq!(decoded.network, Some(Network::Tcp));
    assert_eq!(
        decoded.target,
        Some(Address::Domain("example.com".to_owned()))
    );
    assert_eq!(decoded.port, Some(443));
    assert_eq!(decoded.payload, b"hello");
}

#[test]
fn different_backlog_policies_do_not_share_a_pool_key() {
    let default = pool_key_from_config(
        "trojan.test",
        443,
        "secret",
        Some("trojan.test"),
        false,
        Some("chrome"),
        None,
        MuxResponseBacklogPolicy::default(),
    );
    let tuned = pool_key_from_config(
        "trojan.test",
        443,
        "secret",
        Some("trojan.test"),
        false,
        Some("chrome"),
        None,
        MuxResponseBacklogPolicy::from_config(Some(64), Some(2 * 1024 * 1024))
            .expect("valid policy"),
    );
    assert_ne!(default, tuned);
}

#[test]
fn inbound_writer_enforces_frame_and_byte_limits() {
    let frame_policy = MuxResponseBacklogPolicy::from_config(Some(1), Some(16 * 1024))
        .expect("valid frame policy");
    let (frame_tx, _frame_rx) = tokio::sync::mpsc::channel(frame_policy.frames());
    let frame_writer =
        TrojanInboundMuxWriter::new(frame_tx, MuxResponseBacklog::from_policy(frame_policy));
    frame_writer.frame(vec![1]).expect("first frame");
    assert!(frame_writer.frame(vec![2]).is_err());

    let byte_policy =
        MuxResponseBacklogPolicy::from_config(Some(2), Some(16 * 1024)).expect("valid byte policy");
    let (byte_tx, _byte_rx) = tokio::sync::mpsc::channel(byte_policy.frames());
    let byte_backlog = MuxResponseBacklog::from_policy(byte_policy);
    let byte_writer = TrojanInboundMuxWriter::new(byte_tx, byte_backlog.clone());
    byte_writer.frame(vec![0; 16 * 1024]).expect("byte budget");
    assert_eq!(byte_backlog.used(), 16 * 1024);
    assert!(byte_writer.frame(vec![1]).is_err());
}

#[tokio::test]
async fn eviction_through_a_clone_removes_cached_connections() {
    let pool = TrojanMuxConnectionPool::new();
    let bridge = pool.clone();
    let key = pool_key_from_config(
        "trojan.test",
        443,
        "secret",
        None,
        true,
        None,
        None,
        MuxResponseBacklogPolicy::default(),
    );
    let (stream, _peer) = tokio::io::duplex(64);
    let connection = Arc::new(key.clone().into_pool_conn(stream, 4));
    pool.pool.lock().expect("pool lock").insert(key, connection);
    bridge.evict_all();
    assert!(pool.pool.lock().expect("pool lock").is_empty());
}

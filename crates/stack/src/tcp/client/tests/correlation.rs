use super::*;

#[tokio::test]
async fn atomic_reply_classification_consumes_invalid_state_and_reset_only_for_owned_tuple() {
    let local: IpAddr = "10.0.0.2".parse().unwrap();
    let remote: IpAddr = "10.0.0.1".parse().unwrap();
    let (tx, mut rx) = mpsc::channel(16);
    let client = std::sync::Arc::new(ClientTcpStack::new(vec![local], tx, 1420).unwrap());
    let connecting = {
        let client = client.clone();
        tokio::spawn(async move { client.connect(local, SocketAddr::new(remote, 443)).await })
    };
    let syn = rx.recv().await.unwrap();
    let syn = packet::parse_tcp(&syn).unwrap();
    let unrelated = packet::build_tcp(
        remote,
        local,
        444,
        syn.src.port,
        1,
        0,
        packet::tcp_flags::SYN,
        &[],
    );
    assert!(!client.feed_correlated(&unrelated).await);
    assert!(!client.feed_correlated(b"invalid").await);
    let invalid_ack = packet::build_tcp(
        remote,
        local,
        443,
        syn.src.port,
        1,
        syn.seq,
        packet::tcp_flags::SYN | packet::tcp_flags::ACK,
        &[],
    );
    assert!(client.feed_correlated(&invalid_ack).await);
    assert!(
        !connecting.is_finished(),
        "invalid ACK cannot complete the handshake"
    );
    let rst = packet::build_tcp(
        remote,
        local,
        443,
        syn.src.port,
        1,
        0,
        packet::tcp_flags::RST,
        &[],
    );
    assert!(client.feed_correlated(&rst).await);
    assert!(matches!(
        connecting.await.unwrap(),
        Err(ClientTcpStackError::ConnectionReset)
    ));
    assert!(
        !client.feed_correlated(&rst).await,
        "retired tuple cannot consume new ingress"
    );
}

use std::time::Duration;

use tokio::time::timeout;
use zero_traits::TcpStack;

use super::*;

#[tokio::test]
async fn cancelled_accept_preserves_dequeued_connection() {
    let (outbound, mut responses) = mpsc::channel(256);
    let stack = UserTcpStack::new(outbound, 1_500);
    let client_ip = "10.0.0.2".parse().unwrap();
    let server_ip = "10.0.0.1".parse().unwrap();
    let client_port = 54_321;
    let server_port = 443;

    let syn = packet::build_tcp_with_mss(
        client_ip,
        server_ip,
        client_port,
        server_port,
        1_000,
        0,
        tcp_flags::SYN,
        1_460,
    );
    stack.feed(&syn).await;
    let syn_ack = responses.recv().await.expect("SYN-ACK response");
    let parsed = packet::parse_tcp(&syn_ack).expect("parse SYN-ACK");
    let ack = packet::build_tcp(
        client_ip,
        server_ip,
        client_port,
        server_port,
        1_001,
        parsed.seq.wrapping_add(1),
        tcp_flags::ACK,
        &[],
    );
    stack.feed(&ack).await;

    // Force accept() to dequeue the connection and then wait while validating
    // it. Cancelling at that exact await used to drop the ReadyConn and emit RST.
    let connections = stack.connections.lock().await;
    let mut interrupted = Box::pin(stack.accept());
    tokio::select! {
        biased;
        _ = &mut interrupted => panic!("accept unexpectedly completed while connection state was locked"),
        _ = tokio::time::sleep(Duration::from_millis(10)) => {}
    }
    assert_eq!(
        stack.accept_tx.capacity(),
        64,
        "the interrupted accept must have dequeued the ready connection"
    );
    drop(interrupted);
    drop(connections);

    let (_, source, destination) = timeout(Duration::from_millis(100), stack.accept())
        .await
        .expect("preserved connection accept timed out")
        .expect("preserved connection disappeared");
    assert_eq!(source.port, client_port);
    assert_eq!(destination.port, server_port);
}

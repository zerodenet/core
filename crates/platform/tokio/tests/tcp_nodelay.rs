use tokio::net::TcpListener;
use zero_platform_tokio::TokioSocket;

#[tokio::test]
async fn outbound_tcp_connections_enable_nodelay() {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind listener");
    let address = listener.local_addr().expect("listener address");
    let accept = tokio::spawn(async move { listener.accept().await.expect("accept connection") });

    let socket = TokioSocket::connect_addr(address)
        .await
        .expect("connect socket")
        .into_inner();

    assert!(socket.nodelay().expect("read TCP_NODELAY"));
    accept.await.expect("accept task");
}

#[tokio::test]
async fn hostname_tcp_connections_enable_nodelay() {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind listener");
    let address = listener.local_addr().expect("listener address");
    let accept = tokio::spawn(async move { listener.accept().await.expect("accept connection") });

    let socket = TokioSocket::connect(&address.to_string())
        .await
        .expect("connect socket")
        .into_inner();

    assert!(socket.nodelay().expect("read TCP_NODELAY"));
    accept.await.expect("accept task");
}

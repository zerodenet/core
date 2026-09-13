use zero_platform_tokio::TokioListener;
#[tokio::test]
async fn accepted_sockets_do_not_delay_small_protocol_acknowledgements() {
    let listener = TokioListener::bind("127.0.0.1:0").await.unwrap();
    let client = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (socket, _) = listener.accept().await.unwrap();
    assert!(socket.into_inner().nodelay().unwrap());
    drop(client);
}

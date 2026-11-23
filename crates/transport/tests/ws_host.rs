#![cfg(feature = "ws")]

#[tokio::test]
#[allow(
    clippy::result_large_err,
    reason = "Tungstenite requires an HTTP error response in its handshake callback"
)]
async fn websocket_host_override_replaces_dial_authority_without_duplicate_headers() {
    let (client, server) = tokio::io::duplex(4096);
    let server_task = tokio::spawn(async move {
        tokio_tungstenite::accept_hdr_async(
            server,
            |request: &tokio_tungstenite::tungstenite::handshake::server::Request, response| {
                assert_eq!(request.headers().get_all("host").iter().count(), 1);
                assert_eq!(request.headers()["host"], "landing.example.test:2443");
                assert_eq!(request.uri().path(), "/ws");
                Ok(response)
            },
        )
        .await
        .expect("accept forwarded websocket");
    });
    struct Profile;
    impl zero_traits::WebSocketTransportProfile for Profile {
        fn path(&self) -> &str {
            "/ws"
        }
        fn header_pairs(&self) -> Vec<(String, String)> {
            vec![("hOsT".to_owned(), "landing.example.test:2443".to_owned())]
        }
    }
    let config = Profile;
    let _stream = zero_transport::ws::connect_ws(client, &config, "entry.example.test", 8443)
        .await
        .expect("connect with landing authority");
    server_task.await.unwrap();
}

#[tokio::test]
async fn websocket_shutdown_flushes_the_server_close_through_a_buffered_transport() {
    use futures_util::{SinkExt, StreamExt};
    use tokio::io::{AsyncReadExt, AsyncWriteExt, BufStream};
    use tokio_tungstenite::{tungstenite::protocol::Role, tungstenite::Message, WebSocketStream};

    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        let (client, server) = tokio::io::duplex(4096);
        let server = async {
            let wire =
                WebSocketStream::from_raw_socket(BufStream::new(server), Role::Server, None).await;
            let mut socket = zero_transport::ws::WebSocketSocket::new(wire);
            assert_eq!(socket.read(&mut [0; 1]).await.unwrap(), 0);
            socket.shutdown().await.unwrap();
            socket.shutdown().await.unwrap();
        };
        let client = async {
            let mut socket = WebSocketStream::from_raw_socket(client, Role::Client, None).await;
            socket.send(Message::Close(None)).await.unwrap();
            assert_eq!(socket.next().await.unwrap().unwrap(), Message::Close(None));
        };
        tokio::join!(server, client);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn websocket_rejection_reaches_the_peer_through_a_buffered_transport() {
    use tokio::io::BufStream;
    use tokio_tungstenite::tungstenite::{http::StatusCode, Error};
    use zero_transport::RuntimeError;

    let (client, server) = tokio::io::duplex(4096);
    let (client, server) = tokio::time::timeout(std::time::Duration::from_secs(3), async {
        tokio::join!(
            tokio_tungstenite::client_async("ws://localhost/rejected", client),
            zero_transport::ws::accept_ws(BufStream::new(server), "/accepted"),
        )
    })
    .await
    .unwrap();
    assert!(matches!(client, Err(Error::Http(reply)) if reply.status() == StatusCode::NOT_FOUND));
    assert!(matches!(server, Err(RuntimeError::Io(error))
        if matches!(error.get_ref().and_then(|cause| cause.downcast_ref::<Error>()),
            Some(Error::Http(reply)) if reply.status() == StatusCode::NOT_FOUND)));
}

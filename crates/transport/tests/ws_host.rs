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

use super::*;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn prepared_h2_connection_runs_its_original_driver_inline_and_preserves_real_grpc_headers() {
    timeout(Duration::from_secs(5), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let hunk = encode_grpc_hunk(b"prepared-wire");
        let mut expected = grpc_frame_header(hunk.len()).to_vec();
        expected.extend_from_slice(&hunk);
        let response = expected.clone();
        let server = tokio::spawn(async move {
            let (io, _) = listener.accept().await.unwrap();
            let mut connection = h2::server::handshake(io).await.unwrap();
            let (request, mut respond) = connection.accept().await.unwrap().unwrap();
            assert_eq!(request.uri().path(), "/actual/Many");
            assert_eq!(
                request.uri().authority().unwrap().as_str(),
                "front.example:8443"
            );
            assert_eq!(request.headers()["content-type"], "application/grpc");
            assert!(request.headers()["user-agent"]
                .to_str()
                .unwrap()
                .contains("Firefox/140.0"));
            let response_header = Response::builder()
                .status(200)
                .header("content-type", "application/grpc")
                .header("grpc-status", "0")
                .body(())
                .unwrap();
            let mut stream = respond.send_response(response_header, false).unwrap();
            stream.send_data(Bytes::from(response), true).unwrap();
            drop(stream);
            drop(request);
            connection.graceful_shutdown();
            assert!(
                connection.accept().await.is_none(),
                "only the actual prepared request was admitted"
            );
        });
        let io = tokio::net::TcpStream::connect(address).await.unwrap();
        let profile = OwnedGrpcProfile {
            service_names: vec!["/actual/One|Many".into()],
            multi_mode: true,
            authority: Some("front.example:8443".into()),
            initial_window_size: 262144,
            user_agent: Some("firefox".into()),
            ..Default::default()
        };
        let connection = crate::grpc::prepared::prepare_connection(io, profile.clone(), None)
            .await
            .unwrap();
        assert_eq!(
            connection.active.load(Ordering::Relaxed),
            0,
            "preparation did not open a logical stream or spawn its relays"
        );
        let crate::grpc::prepared::PreparedConnection {
            sender,
            driver,
            active: _,
        } = connection;
        let operation = async move {
            let mut sender = sender.ready().await.unwrap();
            let (response, send) = sender
                .send_request(options::request(&profile, "ignored.example").unwrap(), true)
                .unwrap();
            drop(send);
            let response = response.await.unwrap();
            assert_eq!(response.status(), 200);
            let mut body = response.into_body();
            let mut received = Vec::new();
            while let Some(bytes) = body.data().await {
                let bytes = bytes.unwrap();
                body.flow_control().release_capacity(bytes.len()).unwrap();
                received.extend_from_slice(&bytes);
            }
            assert_eq!(received, expected);
            drop(body);
            drop(sender);
        };
        let (driver, ()) = tokio::join!(driver, operation);
        driver.unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}

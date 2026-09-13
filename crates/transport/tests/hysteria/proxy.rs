use super::*;
use bytes::Buf;
#[tokio::test]
async fn proxy_masquerade_streams_both_directions_before_upload_finishes() {
    timeout(Duration::from_secs(15), async {
        let origin = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = origin.local_addr().unwrap();
        let backend = tokio::spawn(async move {
            let (socket, _) = origin.accept().await.unwrap();
            let service = hyper::service::service_fn(
                |request: http::Request<hyper::body::Incoming>| async move {
                    assert_eq!(request.uri().to_string(), "/origin/item%2Fsub?a=2&q=1");
                    assert_eq!(request.headers()["host"], "front.example");
                    assert!(!request.headers().contains_key("x-forwarded-for"));
                    Ok::<_, std::convert::Infallible>(
                        http::Response::builder()
                            .header("x-origin", "streaming")
                            .body(request.into_body())
                            .unwrap(),
                    )
                },
            );
            let _ = hyper::server::conn::http1::Builder::new()
                .half_close(true)
                .serve_connection(hyper_util::rt::TokioIo::new(socket), service)
                .await;
        });
        let (endpoint, mut profile) = endpoint();
        let listen = endpoint.local_addr().unwrap();
        profile.masquerade = crate::hysteria::Masquerade::proxy(
            &format!("http://{address}/origin?a=2"),
            false,
            false,
        )
        .unwrap();
        let server = tokio::spawn(async move {
            let connection = endpoint.accept().await.unwrap().await.unwrap();
            let incoming = accept_connection(connection.clone(), &profile);
            connection.closed().await;
            drop(incoming);
        });
        let connection = dial(listen, &Profile::new("irrelevant").unwrap()).await;
        let (mut driver, mut requests) =
            h3::client::new(h3_quinn::Connection::new(connection.clone()))
                .await
                .unwrap();
        let driver = tokio::spawn(async move {
            std::future::poll_fn(|cx| driver.poll_close(cx)).await;
        });
        let request = http::Request::builder()
            .method("POST")
            .uri("https://front.example/item%2Fsub?q=1")
            .header("x-forwarded-for", "untrusted")
            .body(())
            .unwrap();
        let mut stream = requests.send_request(request).await.unwrap();
        // A buffering proxy cannot produce these headers until the upload ends.
        let response = stream.recv_response().await.unwrap();
        assert_eq!(response.headers()["x-origin"], "streaming");
        let (mut send, mut recv) = stream.split();
        let expected = 3 * 1024 * 1024 + 7;
        let upload = async {
            let mut left = expected;
            while left > 0 {
                let n = left.min(16384);
                send.send_data(Bytes::from(vec![0x5a; n])).await.unwrap();
                left -= n;
            }
            send.finish().await.unwrap();
        };
        let download = async {
            let mut count = 0;
            while let Some(mut data) = recv.recv_data().await.unwrap() {
                let n = data.remaining();
                let data = data.copy_to_bytes(n);
                assert!(data.iter().all(|b| *b == 0x5a));
                count += n;
            }
            assert_eq!(count, expected);
        };
        tokio::join!(upload, download);
        connection.close(0u32.into(), b"");
        drop(requests);
        driver.abort();
        server.await.unwrap();
        backend.abort();
    })
    .await
    .unwrap();
}

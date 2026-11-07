//! Peer connection completion cannot discard decoded DATA blocked on the real FIFO.
use super::*;

#[tokio::test]
async fn graceful_connection_end_drains_the_same_large_hunk_worker_before_client_eof() {
    timeout(Duration::from_secs(8), async {
        for multi_mode in [false, true] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let payload: Vec<u8> = (0..2 * 1024 * 1024).map(|n| (n % 251) as u8).collect();
            let expected = payload.clone();
            let (closed, actual_peer_closed) = tokio::sync::oneshot::channel();
            let peer = tokio::spawn(async move {
                let (socket, _) = listener.accept().await.unwrap();
                socket.set_nodelay(true).unwrap();
                let mut connection = h2::server::handshake(socket).await.unwrap();
                let (request, mut respond) = connection.accept().await.unwrap().unwrap();
                let response = Response::builder().header("content-type", "application/grpc").body(()).unwrap();
                let mut sender = respond.send_response(response, false).unwrap();
                {
                let operation = async {
                    let mut request = request.into_body();
                    while let Some(bytes) = request.data().await {
                        let bytes = bytes.unwrap();
                        request.flow_control().release_capacity(bytes.len()).unwrap();
                    }
                    // One legal message, rather than 128 tiny messages: decoded
                    // plaintext must exceed the original 64 * 16KiB read FIFO.
                    let hunk = encode_grpc_hunk(&payload);
                    let mut frame = Vec::from(grpc_frame_header(hunk.len()));
                    frame.extend_from_slice(&hunk);
                    let mut frame = Bytes::from(frame);
                    while !frame.is_empty() {
                        sender.reserve_capacity(frame.len());
                        let count = futures_util::future::poll_fn(|cx|sender.poll_capacity(cx)).await.unwrap().unwrap();
                        if count != 0 {
                            let data = frame.split_to(count.min(frame.len()));
                            sender.send_data(data, false).unwrap();
                        }
                    }
                    let mut trailers = http::HeaderMap::new();
                    trailers.insert("grpc-status", http::HeaderValue::from_static("0"));
                    sender.send_trailers(trailers).unwrap();
                };
                tokio::pin!(operation);
                tokio::select! {
                    _ = &mut operation => {},
                    incoming = connection.accept() => panic!("peer connection ended before DATA/trailers: {}",incoming.is_some()),
                }
                }
                drop(sender);
                connection.graceful_shutdown();
                while let Some(incoming) = connection.accept().await {
                    let _ = incoming.unwrap();
                    panic!("no second request is part of this original connection");
                }
                closed.send(()).unwrap();
            });
            let socket = tokio::net::TcpStream::connect(address).await.unwrap();
            socket.set_nodelay(true).unwrap();
            let profile = OwnedGrpcProfile { service_names: vec!["drain/Tun".into()], multi_mode, ..Default::default() };
            let (mut stream, driver) = PreparedSingleClient::prepare(socket, &profile, "localhost", None).await.unwrap().into_parts();
            let driver = tokio::spawn(driver);
            AsyncWriteExt::shutdown(&mut stream).await.unwrap();
            // The actual peer has completed DATA, trailers and graceful GOAWAY
            // before the application consumes any decoded response queue.
            actual_peer_closed.await.unwrap();
            let mut received = Vec::new();
            stream.read_to_end(&mut received).await.unwrap();
            assert!(received == expected, "{} of {} actual reverse bytes, first difference {:?}", received.len(), expected.len(), received.iter().zip(&expected).position(|(left,right)|left!=right));
            drop(stream);
            driver.await.unwrap().unwrap();
            peer.await.unwrap();
        }
    }).await.expect("original eight-second full-data and EOF deadline");
}

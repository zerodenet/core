//! A received HTTP/1 upload survives its original physical connection's EOF.
use super::*;

#[tokio::test]
async fn vless_tcp_xhttp_received_pipelines_keep_bounded_original_scope_and_route_all_packets() {
    timeout(Duration::from_secs(20), exercise()).await.unwrap();
}
async fn exercise() {
    let proxy = fixture::proxy(serde_json::json!({
        "type": "vless", "users": [{"id": USER}],
        "split_http": {"path": "/scope/", "mode": "auto", "sc_max_buffered_posts": 8}
    }))
    .await;
    let echo = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (owner, stop, address) = fixture::install(&proxy);
    let business = async {
        owner.wait_ready().await.unwrap();
        let mut download = TcpStream::connect(address).await.unwrap();
        download
            .write_all(
                format!(
                    "GET /scope/retained?x_padding={} HTTP/1.1\r\nHost: localhost\r\n\r\n",
                    "X".repeat(100)
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        let mut head = Vec::new();
        while !head.ends_with(b"\r\n\r\n") {
            head.push(download.read_u8().await.unwrap());
            assert!(head.len() <= 8192);
        }
        assert!(
            head.starts_with(b"HTTP/1.1 200 "),
            "{:?}",
            String::from_utf8_lossy(&head)
        );

        // Neither VLESS nor the destination can progress without sequence zero.
        // Twenty complete higher packets exceed both bounded eight-entry queues.
        // HTTP/1 must acknowledge their reception and drain its actual EOF while
        // the already-polled admissions retain their original accepted Scope.
        let first = upload(address, 1..=20, echo.local_addr().unwrap().port()).await;
        receipts(first, 20).await;
        let mut pending = Box::pin(echo.accept());
        std::future::poll_fn(|cx| {
            assert!(pending.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        assert!(proxy.engine().active_sessions().is_empty());
        eprintln!("scoped pipeline: received higher packets, actual destination accept pending");

        let mut sequences = vec![0];
        sequences.extend(21..66);
        let last = upload(address, sequences, echo.local_addr().unwrap().port()).await;
        let mut target = pending.await.unwrap().0;
        let read = async {
            for sequence in 0..66u8 {
                let mut packet = [0; 2048];
                target.read_exact(&mut packet).await.unwrap();
                assert!(packet.iter().all(|byte| *byte == sequence));
            }
        };
        tokio::join!(read, receipts(last, 46));
        eprintln!("scoped pipeline: all payloads and HTTP receipts complete");
        assert_eq!(proxy.engine().active_sessions().len(), 1);
        let mut byte = [0];
        let mut waiting = Box::pin(target.read(&mut byte));
        std::future::poll_fn(|cx| {
            assert!(waiting.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        stop.send_replace(true);
        assert_eq!(waiting.await.unwrap(), 0);
        drop(download);
    };
    let (observed, ()) = tokio::join!(owner.observe_driver(), business);
    observed.unwrap();
    fixture::retire(&proxy, 5, 2).await;
    let records = proxy.engine().completed_sessions();
    assert_eq!(records.len(), 1);
    assert!(records[0].bytes_up >= 66 * 2048);
}
async fn upload(
    address: std::net::SocketAddr,
    sequences: impl IntoIterator<Item = u16>,
    port: u16,
) -> TcpStream {
    let mut stream = TcpStream::connect(address).await.unwrap();
    let mut wire = Vec::new();
    for sequence in sequences {
        let mut data = if sequence == 0 {
            request(1, port)
        } else {
            Vec::new()
        };
        data.extend(vec![sequence as u8; 2048]);
        wire.extend(format!(
            "POST /scope/retained/{sequence}?x_padding={} HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n",
            "X".repeat(100), data.len()
        ).as_bytes());
        wire.extend(data);
    }
    stream.write_all(&wire).await.unwrap();
    stream.shutdown().await.unwrap();
    stream
}
async fn receipts(mut stream: TcpStream, expected: usize) {
    let mut replies = String::new();
    stream.read_to_string(&mut replies).await.unwrap();
    let mut count = 0;
    for head in replies.split("HTTP/1.1 ").skip(1) {
        assert!(head.starts_with("200 "), "{head}");
        count += 1;
    }
    assert_eq!(count, expected);
}

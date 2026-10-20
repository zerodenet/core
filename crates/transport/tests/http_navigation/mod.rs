use super::*;

#[tokio::test]
async fn navigation_uses_owned_connection_headers_referers_and_discovered_paths() {
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (done, received) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let mut server = h2::server::handshake(server).await.unwrap();
        let mut requests = Vec::new();
        while let Some(request) = server.accept().await {
            let (request, mut response) = request.unwrap();
            assert_eq!(request.headers()["sec-fetch-mode"], "navigate");
            assert_eq!(request.headers()["cookie"], "padding=0000");
            assert!(request.uri().path() == "/entry" || request.uri().path() == "/child");
            assert!(
                !request.headers().contains_key("referer")
                    || request.headers()["referer"]
                        .to_str()
                        .unwrap()
                        .starts_with("https://nav.test/")
            );
            requests.push(request.uri().path().to_owned());
            let mut body = response
                .send_response(http::Response::new(()), false)
                .unwrap();
            body.send_data(Bytes::from_static(b"<a href=\"/child\">child</a>"), true)
                .unwrap();
            if requests.len() == 7 {
                let _ = done.send(requests);
                break;
            }
        }
    });
    start(
        client,
        "nav.test".into(),
        Plan {
            path: "/entry".into(),
            ranges: [(4, 4), (2, 2), (3, 3), (0, 0), (0, 0)],
        },
    )
    .await;
    let requests = tokio::time::timeout(Duration::from_secs(3), received)
        .await
        .unwrap()
        .unwrap();
    // One first request plus two concurrent workers with three requests each.
    assert_eq!(requests.len(), 7);
    server.await.unwrap();
    let paths = paths::for_host("nav.test", "/ignored");
    let mut paths = paths.lock().await;
    paths.discover(
        "https://nav.test",
        b"href=\"https://attacker.test/x\" href=\"//attacker/x\" href=\"/file.txt\"",
    );
    for _ in 0..20 {
        assert!(matches!(paths.choose().as_str(), "/entry" | "/child"));
    }
}

#[tokio::test]
async fn prepared_navigation_does_not_write_before_executor_and_drop_releases_carrier() {
    use tokio::io::AsyncReadExt;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (mut peer, _) = listener.accept().await.unwrap();
    let prepared = prepare(
        client,
        "cold.test".into(),
        Plan {
            path: "/".into(),
            ranges: [(0, 0), (0, 0), (0, 0), (0, 0), (11, 11)],
        },
    )
    .unwrap();
    let (delay, job) = prepared.into_parts();
    assert_eq!(delay, Duration::from_millis(11));
    // The peer's real read is pending: preparation has not sent an H2 preface.
    let mut byte = [0];
    assert!(
        tokio::time::timeout(Duration::from_millis(20), peer.read(&mut byte))
            .await
            .is_err()
    );
    drop(job);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), peer.read(&mut byte))
            .await
            .unwrap()
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn scoped_executor_drives_real_h2_and_dropping_job_closes_its_original_carrier() {
    use tokio::io::AsyncReadExt;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (mut peer, _) = listener.accept().await.unwrap();
    let (_, mut job) = prepare(
        client,
        "owned.test".into(),
        Plan {
            path: "/".into(),
            ranges: [(0, 0); 5],
        },
    )
    .unwrap()
    .into_parts();
    let mut preface = [0; 24];
    tokio::select! {
        result = &mut job => panic!("navigation exited before actual peer handshake: {result:?}"),
        result = tokio::time::timeout(Duration::from_secs(1), peer.read_exact(&mut preface)) => { result.unwrap().unwrap(); }
    }
    assert_eq!(&preface, b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");
    drop(job);
    let mut remaining = Vec::new();
    tokio::time::timeout(Duration::from_secs(1), peer.read_to_end(&mut remaining))
        .await
        .unwrap()
        .unwrap();
}

mod owned;

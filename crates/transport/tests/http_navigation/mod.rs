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
            if requests.len() == 3 {
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
            ranges: [(4, 4), (1, 1), (2, 2), (0, 0), (0, 0)],
        },
    )
    .await;
    let requests = tokio::time::timeout(Duration::from_secs(3), received)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(requests.len(), 3);
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

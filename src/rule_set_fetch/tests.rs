use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn document(directory: &Path, address: std::net::SocketAddr, interval: u64) -> PathBuf {
    let path = directory.join("config.json");
    fs::write(&path, serde_json::json!({
        "route": {"rule_sets": [{"tag":"remote", "type":"url", "path":"rules.txt",
            "url":format!("http://{address}/rules"),"format":"domain_list","update_interval_seconds":interval}],
            "final":{"type":"direct"}}
    }).to_string()).unwrap();
    path
}
async fn request(stream: &mut tokio::net::TcpStream) -> String {
    let mut header = Vec::new();
    while !header.ends_with(b"\r\n\r\n") {
        header.push(stream.read_u8().await.unwrap());
    }
    String::from_utf8(header).unwrap()
}

#[tokio::test]
async fn actual_redirect_chunked_cache_and_fresh_cache_preserve_relative_rules() {
    let directory = tempfile::tempdir().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let path = document(directory.path(), listener.local_addr().unwrap(), 3600);
    let mut configuration: serde_json::Value =
        serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    configuration["route"]["rule_sets"][0]["url"] = serde_json::Value::String(format!(
        "http://user:pass@{}/rules",
        listener.local_addr().unwrap()
    ));
    fs::write(&path, configuration.to_string()).unwrap();
    let server = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        let header = request(&mut first).await;
        assert!(header.starts_with("GET /rules HTTP/1.1"));
        assert!(header
            .to_ascii_lowercase()
            .contains("authorization: basic dxnlcjpwyxnz"));
        first
            .write_all(b"HTTP/1.1 302 Found\r\nLocation: /next\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
        // The second address attempt is driven only after the actual owner
        // retires the first exchange; generic owner tests inspect its receipts.
        let mut byte = [0];
        assert_eq!(first.read(&mut byte).await.unwrap(), 0);
        let (mut next, _) = listener.accept().await.unwrap();
        let header = request(&mut next).await;
        assert!(header.starts_with("GET /next HTTP/1.1"));
        assert!(header
            .to_ascii_lowercase()
            .contains("authorization: basic dxnlcjpwyxnz"));
        next.write_all(
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nC\r\nexample.com\n\r\n0\r\n\r\n",
        )
        .await
        .unwrap();
    });
    let slot = BootstrapSlot::new();
    let source = Arc::new(ConfigurationSource::new(&path));
    let config = slot
        .load(source.clone(), path.to_str().unwrap())
        .await
        .unwrap();
    server.await.unwrap();
    assert!(slot.original.lock().unwrap().is_none());
    source.confirm_original_io_closed().unwrap();
    assert_eq!(
        fs::read(directory.path().join("rules.txt")).unwrap(),
        b"example.com\n"
    );
    assert_eq!(
        Path::new(&config.route.rule_sets[0].path),
        directory.path().join("rules.txt")
    );
    // Listener is gone: a fresh immutable cache must require no new dial.
    slot.load(source.clone(), path.to_str().unwrap())
        .await
        .unwrap();
    source.confirm_original_io_closed().unwrap();
    assert!(fs::read_dir(directory.path()).unwrap().all(|entry| !entry
        .unwrap()
        .file_name()
        .to_string_lossy()
        .contains("download")));
}

#[tokio::test]
async fn actual_http_failure_closes_candidate_before_handing_off_stale_cache() {
    let directory = tempfile::tempdir().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let path = document(directory.path(), listener.local_addr().unwrap(), 0);
    fs::write(directory.path().join("rules.txt"), b"old.example\n").unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        request(&mut stream).await;
        stream
            .write_all(b"HTTP/1.1 503 Unavailable\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
        let mut byte = [0];
        assert_eq!(stream.read(&mut byte).await.unwrap(), 0);
    });
    let source = Arc::new(ConfigurationSource::new(&path));
    let slot = BootstrapSlot::new();
    let config = slot
        .load(source.clone(), path.to_str().unwrap())
        .await
        .unwrap();
    server.await.unwrap();
    source.confirm_original_io_closed().unwrap();
    assert!(slot.original.lock().unwrap().is_none());
    assert_eq!(
        fs::read(&config.route.rule_sets[0].path).unwrap(),
        b"old.example\n"
    );
}

#[tokio::test]
async fn cancelled_pending_download_retains_originals_until_explicit_join_and_close() {
    let directory = tempfile::tempdir().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let path = document(directory.path(), listener.local_addr().unwrap(), 0);
    let source = Arc::new(ConfigurationSource::new(&path));
    let slot = BootstrapSlot::new();
    let mut loading = Box::pin(slot.load(source.clone(), path.to_str().unwrap()));
    let (mut peer, _) = tokio::select! {
        peer = listener.accept() => peer.unwrap(),
        result = &mut loading => panic!("real pending download returned before headers: {result:?}"),
    };
    let mut reading = Box::pin(request(&mut peer));
    tokio::select! {
        _ = &mut reading => {},
        result = &mut loading => panic!("download completed before actual response: {result:?}"),
    }
    drop(reading);
    std::future::poll_fn(|cx| {
        assert!(
            std::future::Future::poll(loading.as_mut(), cx).is_pending(),
            "actual HTTP header read must remain pending"
        );
        std::task::Poll::Ready(())
    })
    .await;
    // Drop the actual initializer future, not its retained original owner.
    drop(loading);
    let original = slot.original.lock().unwrap().clone().unwrap();
    assert!(original.cache.lock().unwrap().is_some());
    assert!(source.confirm_original_io_closed().is_err());
    original.cleanup().await.unwrap();
    source.confirm_original_io_closed().unwrap();
    assert!(!directory.path().join("rules.txt").exists());
    let mut byte = [0];
    assert_eq!(peer.read(&mut byte).await.unwrap(), 0);
}

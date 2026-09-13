use super::*;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[test]
fn ocsp_request_matches_pinned_go_encoding_and_certificate_endpoints() {
    let leaf = include_bytes!("../fixtures/ocsp/leaf.der");
    let issuer = include_bytes!("../fixtures/ocsp/issuer.der");
    assert_eq!(
        request::encode(leaf, issuer).unwrap(),
        include_bytes!("../fixtures/ocsp/request.der")
    );
    assert_eq!(
        request::endpoints(leaf).unwrap(),
        (
            "http://ocsp.test/status".into(),
            Some("http://issuer.test/ca.der".into())
        )
    );
    assert!(request::encode(leaf, leaf).is_err());
    assert!(request::encode(leaf, &[issuer.as_slice(), &[0]].concat()).is_err());
}

async fn accept(listener: &tokio::net::TcpListener) -> tokio::net::TcpStream {
    tokio::time::timeout(Duration::from_secs(5), listener.accept())
        .await
        .unwrap()
        .unwrap()
        .0
}

async fn read_request(stream: &mut tokio::net::TcpStream) -> (String, Vec<u8>) {
    let mut bytes = Vec::new();
    loop {
        bytes.push(stream.read_u8().await.unwrap());
        if bytes.ends_with(b"\r\n\r\n") {
            break;
        }
        assert!(bytes.len() < 8192);
    }
    let header = String::from_utf8(bytes).unwrap();
    let length = header
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().unwrap())
        })
        .unwrap_or(0);
    let mut body = vec![0; length];
    stream.read_exact(&mut body).await.unwrap();
    (header, body)
}

#[tokio::test]
async fn ocsp_http_preserves_post_on_redirect_and_bounds_response_body() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/first", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        for response in [
            "HTTP/1.1 307 Temporary Redirect\r\nLocation: /second\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_owned(),
            "HTTP/1.1 200 OK\r\nContent-Length: 6\r\nConnection: close\r\n\r\nstaple".to_owned(),
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n8\r\n12345678\r\n0\r\n\r\n".to_owned(),
        ] {
            let mut stream = accept(&listener).await;
            let (header, body) = read_request(&mut stream).await;
            assert!(header.starts_with("POST "));
            assert!(header.to_ascii_lowercase().contains("content-type: application/ocsp-request"));
            assert_eq!(body, b"request");
            stream.write_all(response.as_bytes()).await.unwrap();
        }
    });
    let client = HttpClient::new().unwrap();
    assert_eq!(
        fetch(&client, &url, Some(b"request".to_vec()), 6)
            .await
            .unwrap(),
        b"staple"
    );
    assert!(fetch(&client, &url, Some(b"request".to_vec()), 6)
        .await
        .is_err());
    assert!(fetch(&client, "file:///issuer.der", None, 1024)
        .await
        .is_err());
    server.await.unwrap();
}

fn field(tag: u8, body: &[u8]) -> Vec<u8> {
    assert!(body.len() < 128);
    [&[tag, body.len() as u8], body].concat()
}
fn material(url: &str) -> (rcgen::Certificate, rcgen::KeyPair, rcgen::Certificate) {
    let issuer_key = rcgen::KeyPair::generate().unwrap();
    let mut issuer = rcgen::CertificateParams::new(vec![]).unwrap();
    issuer.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca = issuer.self_signed(&issuer_key).unwrap();
    let key = rcgen::KeyPair::generate().unwrap();
    let mut leaf = rcgen::CertificateParams::new(vec!["ocsp.test".into()]).unwrap();
    let mut access = Vec::new();
    for (kind, url) in [(1, format!("{url}/status")), (2, format!("{url}/issuer"))] {
        let mut description = vec![6, 8, 0x2b, 6, 1, 5, 5, 7, 0x30, kind];
        description.extend(field(0x86, url.as_bytes()));
        access.extend(field(0x30, &description));
    }
    leaf.custom_extensions
        .push(rcgen::CustomExtension::from_oid_content(
            &[1, 3, 6, 1, 5, 5, 7, 1, 1],
            field(0x30, &access),
        ));
    let leaf = leaf
        .signed_by(&key, &rcgen::Issuer::from_params(&issuer, &issuer_key))
        .unwrap();
    (leaf, key, ca)
}

#[tokio::test]
async fn ocsp_retrieves_missing_issuer_then_posts_request() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (leaf, _, issuer) = material(&format!("http://{}", listener.local_addr().unwrap()));
    let expected = request::encode(leaf.der(), issuer.der()).unwrap();
    let server = tokio::spawn(async move {
        for (method, path, data) in [
            ("GET", "/issuer", issuer.der().to_vec()),
            ("POST", "/status", b"response".to_vec()),
        ] {
            let mut stream = accept(&listener).await;
            let (header, body) = read_request(&mut stream).await;
            assert!(header.starts_with(&format!("{method} {path} ")));
            if method == "POST" {
                assert_eq!(body, expected);
            } else {
                assert!(body.is_empty());
            }
            stream
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        data.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            stream.write_all(&data).await.unwrap();
        }
    });
    assert_eq!(
        retrieve(&HttpClient::new().unwrap(), &[leaf.der().clone()])
            .await
            .unwrap(),
        b"response"
    );
    server.await.unwrap();
}

#[tokio::test]
async fn automatic_ocsp_publishes_staple_and_resolver_drop_cancels_worker() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (leaf, key, issuer) = material(&format!("http://{}", listener.local_addr().unwrap()));
    let dir = std::env::temp_dir().join(format!("zero-ocsp-{}", rand::random::<u64>()));
    std::fs::create_dir(&dir).unwrap();
    let files = zero_traits::TlsCertificateFiles {
        cert_path: dir.join("cert.pem").to_string_lossy().into_owned(),
        key_path: dir.join("key.pem").to_string_lossy().into_owned(),
        ocsp_path: None,
        ocsp_stapling_secs: 1,
        usage: zero_traits::TlsCertificateUsage::Encipherment,
        build_chain: false,
    };
    std::fs::write(&files.cert_path, leaf.pem() + &issuer.pem()).unwrap();
    std::fs::write(&files.key_path, key.serialize_pem()).unwrap();
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let initial = super::super::server::load(&files, &provider).unwrap();
    let resolver = super::super::refresh::Certificates::new(
        vec![initial],
        vec![files.clone()],
        provider,
        &Default::default(),
    );
    let mut stream = accept(&listener).await;
    let _ = read_request(&mut stream).await;
    stream
        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\nConnection: close\r\n\r\nstaple")
        .await
        .unwrap();
    drop(stream);
    tokio::time::timeout(Duration::from_secs(5), async {
        while resolver.current()[0].key.ocsp.as_deref() != Some(b"staple") {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut failed = accept(&listener).await;
    let _ = read_request(&mut failed).await;
    let (replacement, key, issuer) =
        material(&format!("http://{}", listener.local_addr().unwrap()));
    std::fs::write(&files.cert_path, replacement.pem() + &issuer.pem()).unwrap();
    std::fs::write(&files.key_path, key.serialize_pem()).unwrap();
    failed
        .write_all(b"HTTP/1.1 503 Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    drop(failed);
    let mut failed_replacement = accept(&listener).await;
    let _ = read_request(&mut failed_replacement).await;
    // The failed refresh of the same leaf retains its last staple.
    assert_eq!(
        resolver.current()[0].key.ocsp.as_deref(),
        Some(b"staple".as_slice())
    );
    failed_replacement
        .write_all(b"HTTP/1.1 503 Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    drop(failed_replacement);
    tokio::time::timeout(Duration::from_secs(5), async {
        while resolver.current()[0].key.cert[0] != *replacement.der() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        resolver.current()[0].key.ocsp.is_none(),
        "replacement must not inherit the old leaf's staple"
    );
    // Begin the next response, then drop its only lifecycle owner while HTTP is pending.
    let mut pending = accept(&listener).await;
    let _ = read_request(&mut pending).await;
    drop(resolver);
    let mut byte = [0];
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), pending.read(&mut byte))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    std::fs::remove_dir_all(dir).unwrap();
}

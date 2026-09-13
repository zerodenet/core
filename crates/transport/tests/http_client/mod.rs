use super::*;

#[tokio::test]
async fn https_origin_negotiates_http2_and_streams_response() {
    use hyper_util::rt::TokioIo;
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let key = rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(vec![cert.cert.der().clone()], key.into())
    .unwrap();
    config.alpn_protocols = vec![b"h2".to_vec()];
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let tls = tokio_rustls::TlsAcceptor::from(Arc::new(config))
            .accept(socket)
            .await
            .unwrap();
        assert_eq!(tls.get_ref().1.alpn_protocol(), Some(b"h2".as_slice()));
        let service = hyper::service::service_fn(
            |request: http::Request<hyper::body::Incoming>| async move {
                assert_eq!(request.version(), http::Version::HTTP_2);
                assert_eq!(request.uri().path(), "/origin");
                Ok::<_, std::convert::Infallible>(http::Response::new(Full::new(
                    Bytes::from_static(b"h2 origin"),
                )))
            },
        );
        hyper::server::conn::http2::Builder::new(TokioExecutor::new())
            .serve_connection(TokioIo::new(tls), service)
            .await
            .unwrap();
    });
    let client = HttpClient::with_insecure(true).unwrap();
    let response = tokio::time::timeout(
        Duration::from_secs(3),
        client.send(
            http::Request::builder()
                .uri(format!("https://localhost:{}/origin", address.port()))
                .body(Bytes::new())
                .unwrap(),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(response.version(), http::Version::HTTP_2);
    let mut body = response.into_body();
    assert_eq!(body.data().await.unwrap().unwrap(), "h2 origin");
    assert!(body.data().await.unwrap().is_none());
    server.abort();
}

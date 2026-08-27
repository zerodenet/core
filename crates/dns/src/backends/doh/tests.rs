use std::sync::Arc;

use bytes::Bytes;
use tokio::net::TcpListener;
use zero_traits::IpAddress;

use super::*;

#[tokio::test]
async fn exchanges_a_dns_message_over_bound_http2_transport() {
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
        .expect("generate certificate");
    let certificate = certified.cert.der().clone();
    let key = rustls::pki_types::PrivatePkcs8KeyDer::from(certified.signing_key.serialize_der());

    let mut server_tls = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .expect("server TLS protocol versions")
    .with_no_client_auth()
    .with_single_cert(vec![certificate.clone()], key.into())
    .expect("build server TLS");
    server_tls.alpn_protocols = vec![b"h2".to_vec()];
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_tls));
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind DoH test server");
    let address = listener.local_addr().expect("DoH test address");
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept DoH TCP");
        let stream = acceptor.accept(stream).await.expect("accept DoH TLS");
        let mut connection = h2::server::handshake(stream).await.expect("accept HTTP/2");
        for _ in 0..2 {
            let (request, mut respond) = connection
                .accept()
                .await
                .expect("receive HTTP/2 request")
                .expect("valid HTTP/2 request");
            assert_eq!(request.uri().path(), "/dns-query");
            let mut body = request.into_body();
            let mut query = Vec::new();
            while let Some(chunk) = body.data().await {
                let chunk = chunk.expect("read DoH request body");
                query.extend_from_slice(&chunk);
                body.flow_control()
                    .release_capacity(chunk.len())
                    .expect("release request capacity");
            }
            let response = crate::message::build_address_response(
                &query,
                &[IpAddress::V4([203, 0, 113, 53])],
                120,
            );
            let headers = http::Response::builder()
                .status(200)
                .header("content-type", "application/dns-message")
                .body(())
                .expect("build DoH response");
            let mut send = respond
                .send_response(headers, false)
                .expect("send DoH headers");
            send.send_data(Bytes::from(response), true)
                .expect("send DoH response body");
        }
        let _ = tokio::time::timeout(std::time::Duration::from_secs(1), connection.accept()).await;
    });

    let mut roots = rustls::RootCertStore::empty();
    roots.add(certificate).expect("trust test certificate");
    let mut client_tls = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .expect("client TLS protocol versions")
    .with_root_certificates(roots)
    .with_no_client_auth();
    client_tls.alpn_protocols = vec![b"h2".to_vec()];
    let resolver = DohDnsResolver {
        port: address.port(),
        path: "/dns-query".to_owned(),
        addrs: vec![address],
        server_name: "localhost".to_owned(),
        tls: Arc::new(client_tls),
        egress: zero_platform_tokio::EgressInterfaceControl::default(),
        clients: tokio::sync::Mutex::new(Vec::new()),
        connect_lock: tokio::sync::Mutex::new(()),
    };
    let query = crate::message::build_query("doh.example", crate::message::TYPE_A)
        .expect("build DNS query");

    let response = resolver
        .exchange(&query, None, None)
        .await
        .expect("exchange DoH query");

    let parsed = crate::message::parse_response(&query, &response).expect("parse DNS response");
    assert_eq!(parsed.addresses, vec![IpAddress::V4([203, 0, 113, 53])]);
    let second = resolver
        .exchange(&query, None, None)
        .await
        .expect("reuse DoH HTTP/2 connection");
    let parsed = crate::message::parse_response(&query, &second).expect("parse reused response");
    assert_eq!(parsed.addresses, vec![IpAddress::V4([203, 0, 113, 53])]);
    server.await.expect("DoH server task");
}

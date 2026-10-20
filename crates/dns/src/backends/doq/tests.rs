use super::*;
use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    time::{timeout, Instant},
};
use zero_traits::IpAddress;

fn fixture() -> (quinn::Endpoint, DoqDnsResolver) {
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
        .expect("generate certificate");
    let certificate = certified.cert.der().clone();
    let key = rustls::pki_types::PrivatePkcs8KeyDer::from(certified.signing_key.serialize_der());
    let mut server_tls = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(vec![certificate.clone()], key.into())
    .unwrap();
    server_tls.alpn_protocols = vec![b"doq".to_vec()];
    let server_quic = QuicServerConfig::try_from(server_tls).unwrap();
    let endpoint = quinn::Endpoint::server(
        quinn::ServerConfig::with_crypto(Arc::new(server_quic)),
        "127.0.0.1:0".parse().unwrap(),
    )
    .unwrap();
    let addr = endpoint.local_addr().unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(certificate).unwrap();
    let mut client_tls = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    client_tls.alpn_protocols = vec![b"doq".to_vec()];
    let mut resolver = DoqDnsResolver::new(
        "localhost".to_owned(),
        addr.port(),
        vec![addr.ip()],
        Some("localhost".to_owned()),
        zero_platform_tokio::EgressInterfaceControl::default(),
    )
    .unwrap();
    resolver.client =
        quinn::ClientConfig::new(Arc::new(QuicClientConfig::try_from(client_tls).unwrap()));
    (endpoint, resolver)
}

#[tokio::test]
async fn exchanges_a_framed_dns_message_and_joins_actual_drivers() {
    let (endpoint, resolver) = fixture();
    let server = tokio::spawn(async move {
        let connection = endpoint.accept().await.unwrap().await.unwrap();
        let (mut send, mut recv) = connection.accept_bi().await.unwrap();
        let size = recv.read_u16().await.unwrap() as usize;
        let mut query = vec![0_u8; size];
        recv.read_exact(&mut query).await.unwrap();
        let response = crate::message::build_address_response(
            &query,
            &[IpAddress::V4([203, 0, 113, 53])],
            120,
        );
        send.write_u16(response.len() as u16).await.unwrap();
        send.write_all(&response).await.unwrap();
        send.finish().unwrap();
        send.stopped().await.unwrap();
        connection.closed().await;
        endpoint.close(0u32.into(), b"test finished");
        endpoint.wait_idle().await;
    });
    let query = crate::message::build_query("doq.example", crate::message::TYPE_A).unwrap();
    let response = timeout(Duration::from_secs(8), resolver.exchange(&query))
        .await
        .unwrap()
        .unwrap();
    let parsed = crate::message::parse_response(&query, &response).unwrap();
    assert_eq!(parsed.addresses, vec![IpAddress::V4([203, 0, 113, 53])]);
    let owners = resolver.owners.lock().unwrap().doq_snapshot();
    assert_eq!(owners.len(), 1);
    assert!(owners[0].drained(), "normal response confirms driver joins");
    timeout(Duration::from_secs(8), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn cancelled_pending_query_retains_endpoint_until_real_close_and_driver_join() {
    let (endpoint, resolver) = fixture();
    let resolver = Arc::new(resolver);
    let registry = resolver.owners.lock().unwrap().clone();
    let (entered, pending) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let connection = endpoint.accept().await.unwrap().await.unwrap();
        let (_send, mut recv) = connection.accept_bi().await.unwrap();
        let size = recv.read_u16().await.unwrap() as usize;
        let mut query = vec![0_u8; size];
        recv.read_exact(&mut query).await.unwrap();
        entered.send(()).unwrap();
        // The request has actually reached the peer; no response is fabricated.
        connection.closed().await;
        endpoint.close(0u32.into(), b"test finished");
        endpoint.wait_idle().await;
    });
    let caller = tokio::spawn({
        let resolver = resolver.clone();
        async move {
            let query =
                crate::message::build_query("pending.doq.example", crate::message::TYPE_A).unwrap();
            resolver.exchange(&query).await
        }
    });
    timeout(Duration::from_secs(5), pending)
        .await
        .unwrap()
        .unwrap();
    let owners = registry.doq_snapshot();
    assert_eq!(owners.len(), 1);
    let owner = owners[0].clone();
    assert_eq!(
        owner.task_count(),
        2,
        "one actual endpoint and connection driver"
    );
    let local_addr = owner.local_addr();
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    drop(resolver);
    assert!(owner.closing());
    registry
        .shutdown_doq(Instant::now() + Duration::from_secs(8))
        .await
        .unwrap();
    assert!(owner.drained());
    assert_eq!(owner.task_count(), 0);
    // The original descriptor is gone after join, including the last Endpoint.
    let rebound = std::net::UdpSocket::bind(local_addr).expect("old DoQ socket released");
    drop(rebound);
    timeout(Duration::from_secs(8), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn confirmed_empty_shutdown_rejects_late_actual_endpoint_setup() {
    let (endpoint, resolver) = fixture();
    let owner = owner::EndpointOwner::new();
    owner
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    let source = owner
        .setup(
            endpoint.local_addr().unwrap(),
            resolver.client.clone(),
            &resolver.egress,
        )
        .expect_err("closed owner must not bind or spawn after shutdown confirmation");
    assert_eq!(source.kind(), io::ErrorKind::NotConnected);
    assert_eq!(owner.task_count(), 0);
    assert!(owner.drained());
    endpoint.close(0u32.into(), b"test finished");
    endpoint.wait_idle().await;
}

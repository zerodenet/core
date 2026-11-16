//! Real declared File originals and signed TLS wire; no standalone loader fallback.
use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_platform_tokio::contract::{packet::BoxPacketSession, BoxFuture};
struct Files(zero_platform_tokio::file::declared::DeclaredFileReads);
fn denied<T>() -> BoxFuture<'static, Result<T, ChannelError>>
where
    T: Send + 'static,
{
    Box::pin(async {
        Err(ChannelError::new(
            Stage::Prepare,
            Reason::Unsupported,
            Impact::Component,
        ))
    })
}
impl ResourceAccess for Files {
    fn read_file(
        &self,
        path: std::path::PathBuf,
        limit: usize,
    ) -> BoxFuture<'_, Result<Vec<u8>, ChannelError>> {
        self.0.read(path, limit)
    }
    fn listen_tcp(
        &self,
        _: zero_core::request::Destination,
    ) -> BoxFuture<'_, Result<zero_platform_tokio::ControlledTcpListener, ChannelError>> {
        denied()
    }
    fn listen_datagram(
        &self,
        _: zero_core::request::Destination,
    ) -> BoxFuture<'_, Result<GrantedDatagramListener, ChannelError>> {
        denied()
    }
    fn open_device(&self, _: Arc<str>) -> BoxFuture<'_, Result<BoxPacketSession, ChannelError>> {
        denied()
    }
}
async fn peer(plan: &PreparedServer, trust: rustls::RootCertStore) -> Vec<u8> {
    let (client, server) = tokio::io::duplex(4096);
    let acceptor = plan.acceptor();
    let server = tokio::spawn(async move {
        let mut io = super::super::accept_tls_handshake(&acceptor, server)
            .await
            .unwrap();
        let mut bytes = [0];
        io.read_exact(&mut bytes).await.unwrap();
        io.write_all(&bytes).await.unwrap();
        io.shutdown().await.unwrap();
    });
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(trust)
    .with_no_client_auth();
    let mut io = tokio_rustls::TlsConnector::from(Arc::new(config))
        .connect(
            rustls::pki_types::ServerName::try_from("localhost").unwrap(),
            client,
        )
        .await
        .unwrap();
    let leaf = io.get_ref().1.peer_certificates().unwrap()[0].to_vec();
    io.write_all(b"x").await.unwrap();
    let mut echo = [0];
    io.read_exact(&mut echo).await.unwrap();
    assert_eq!(echo, [b'x']);
    io.shutdown().await.unwrap();
    server.await.unwrap();
    leaf
}
#[tokio::test]
async fn failed_replacement_keeps_last_good_signed_context_and_recovery_closes_same_files() {
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        for backend in [
            zero_traits::TlsBackend::Rustls,
            zero_traits::TlsBackend::OpenSsl,
        ] {
            let directory = tempfile::tempdir().unwrap();
            let cert = directory.path().join("cert.pem");
            let key = directory.path().join("key.pem");
            let first = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
            let second = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
            std::fs::write(&cert, first.cert.pem()).unwrap();
            std::fs::write(&key, first.signing_key.serialize_pem()).unwrap();
            let profile = OwnedServerTlsProfile {
                cert_path: cert.to_string_lossy().into_owned(),
                key_path: key.to_string_lossy().into_owned(),
                alpn: Vec::new(),
                server_fingerprint: None,
                options: zero_traits::ServerTlsOptions {
                    backend,
                    ..Default::default()
                },
            };
            let plan = PreparedServer::new(&profile, None, false).unwrap();
            plan.set_file_read_limit(NonZeroUsize::new(4096).unwrap())
                .unwrap();
            let files = Files(
                zero_platform_tokio::file::declared::DeclaredFileReads::new(
                    [(cert.clone(), 4096), (key.clone(), 4096)],
                    CancellationToken::new(),
                )
                .unwrap(),
            );
            files.0.activate();
            let mut trust = rustls::RootCertStore::empty();
            trust.add(first.cert.der().clone()).unwrap();
            trust.add(second.cert.der().clone()).unwrap();
            plan.load_initial(&files).await.unwrap();
            assert_eq!(peer(&plan, trust.clone()).await, first.cert.der().as_ref());
            std::fs::write(&cert, second.cert.pem()).unwrap();
            std::fs::write(&key, b"not a private key").unwrap();
            assert!(plan.load_initial(&files).await.is_err());
            assert_eq!(
                peer(&plan, trust.clone()).await,
                first.cert.der().as_ref(),
                "failed parsing cannot publish a partial SNI/key context"
            );
            std::fs::write(&key, second.signing_key.serialize_pem()).unwrap();
            plan.load_initial(&files).await.unwrap();
            assert_eq!(peer(&plan, trust).await, second.cert.der().as_ref());
            let report = files
                .0
                .shutdown(tokio::time::Instant::now() + std::time::Duration::from_secs(3))
                .await
                .unwrap();
            assert!(report.is_complete());
            assert_eq!(report.closed_io.len(), 2);
            assert_eq!(report.released_resources.len(), 2);
            assert!(
                plan.library.lock().unwrap().is_none(),
                "no private library worker or raw file loader was adopted"
            );
        }
    })
    .await
    .unwrap();
}

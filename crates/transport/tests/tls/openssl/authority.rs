use std::{io, pin::Pin, time::Duration};

use openssl::ssl::{Ssl, SslContextBuilder, SslMethod, SslVerifyMode};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_traits::{ServerTlsOptions, TlsBackend, TlsCertificateFiles, TlsCertificateUsage};

use crate::profile::OwnedServerTlsProfile;

fn write_authority(directory: &std::path::Path) -> (String, String) {
    let key = rcgen::KeyPair::generate().unwrap();
    let mut parameters = rcgen::CertificateParams::new(vec![]).unwrap();
    parameters.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let certificate = parameters.self_signed(&key).unwrap();
    let cert_path = directory.join("authority.pem");
    let key_path = directory.join("authority.key");
    std::fs::write(&cert_path, certificate.pem()).unwrap();
    std::fs::write(&key_path, key.serialize_pem()).unwrap();
    (
        cert_path.to_string_lossy().into_owned(),
        key_path.to_string_lossy().into_owned(),
    )
}

fn profile(cert_path: String, key_path: String) -> OwnedServerTlsProfile {
    OwnedServerTlsProfile {
        options: ServerTlsOptions {
            backend: TlsBackend::OpenSsl,
            reload_interval_secs: 1,
            certificates: vec![TlsCertificateFiles {
                cert_path,
                key_path,
                usage: TlsCertificateUsage::AuthorityIssue,
                build_chain: true,
                ..Default::default()
            }],
            ..Default::default()
        },
        cert_path: String::new(),
        key_path: String::new(),
        alpn: Vec::new(),
        server_fingerprint: None,
    }
}

async fn handshake_certificate(
    context: &super::super::super::OpenSslServerContext,
) -> io::Result<Vec<u8>> {
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let server = async {
        let mut stream =
            super::super::super::stream::OpenSslTlsStream::accept(context, server_io).await?;
        let mut byte = [0];
        stream.read_exact(&mut byte).await?;
        Ok::<_, io::Error>(())
    };
    let client = async {
        let mut builder = SslContextBuilder::new(SslMethod::tls_client()).unwrap();
        builder.set_verify(SslVerifyMode::NONE);
        let mut ssl = Ssl::new(&builder.build()).unwrap();
        ssl.set_hostname("issued.example").unwrap();
        let mut stream = tokio_openssl::SslStream::new(ssl, client_io).unwrap();
        Pin::new(&mut stream).connect().await.unwrap();
        let certificate = stream.ssl().peer_certificate().unwrap().to_der().unwrap();
        stream.write_all(b"x").await.unwrap();
        stream.flush().await.unwrap();
        Ok::<_, io::Error>(certificate)
    };
    let (server, client) = tokio::join!(server, client);
    server?;
    client
}

#[tokio::test]
async fn openssl_authority_issues_caches_and_refreshes_live_certificates() {
    let directory = tempfile::tempdir().unwrap();
    let (cert_path, key_path) = write_authority(directory.path());
    let context = super::super::super::OpenSslServerContext::build(
        &profile(cert_path.clone(), key_path.clone()),
        None,
    )
    .unwrap();

    let original = handshake_certificate(&context).await.unwrap();
    assert_eq!(handshake_certificate(&context).await.unwrap(), original);

    let _replacement = write_authority(directory.path());
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if handshake_certificate(&context).await.unwrap() != original {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
}

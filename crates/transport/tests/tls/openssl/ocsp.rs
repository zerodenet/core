use std::{io, pin::Pin, time::Duration};

use openssl::{
    ocsp::{OcspResponse, OcspResponseStatus},
    ssl::{Ssl, SslContextBuilder, SslMethod, SslVerifyMode, StatusType},
};
use rcgen::{DistinguishedName, DnType};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_traits::{ServerTlsOptions, TlsBackend, TlsParameters};

use super::Material;

fn field(tag: u8, body: &[u8]) -> Vec<u8> {
    assert!(body.len() < 128);
    [&[tag, body.len() as u8], body].concat()
}

fn material(url: &str) -> Material {
    let issuer_key = rcgen::KeyPair::generate().unwrap();
    let mut issuer = rcgen::CertificateParams::new(vec![]).unwrap();
    issuer.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    issuer.distinguished_name = distinguished_name("OCSP test issuer");
    let issuer_cert = issuer.self_signed(&issuer_key).unwrap();
    let key = rcgen::KeyPair::generate().unwrap();
    let mut leaf = rcgen::CertificateParams::new(vec!["ocsp.test".into()]).unwrap();
    leaf.distinguished_name = distinguished_name("OCSP test leaf");
    let mut description = vec![6, 8, 0x2b, 6, 1, 5, 5, 7, 0x30, 1];
    description.extend(field(0x86, format!("{url}/status").as_bytes()));
    leaf.custom_extensions
        .push(rcgen::CustomExtension::from_oid_content(
            &[1, 3, 6, 1, 5, 5, 7, 1, 1],
            field(0x30, &field(0x30, &description)),
        ));
    let leaf = leaf
        .signed_by(&key, &rcgen::Issuer::from_params(&issuer, &issuer_key))
        .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let cert_path = directory.path().join("server.pem");
    let key_path = directory.path().join("server.key");
    std::fs::write(&cert_path, leaf.pem() + &issuer_cert.pem()).unwrap();
    std::fs::write(&key_path, key.serialize_pem()).unwrap();
    Material {
        _directory: directory,
        cert_path: cert_path.to_string_lossy().into_owned(),
        key_path: key_path.to_string_lossy().into_owned(),
    }
}

fn distinguished_name(common_name: &str) -> DistinguishedName {
    let mut name = DistinguishedName::new();
    name.push(DnType::CommonName, common_name);
    name
}

async fn read_request(stream: &mut tokio::net::TcpStream) {
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
        .find_map(|line| line.split_once(':'))
        .filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .map(|(_, value)| value.trim().parse::<usize>().unwrap())
        .unwrap_or(0);
    let mut body = vec![0; length];
    stream.read_exact(&mut body).await.unwrap();
}

async fn assert_automatic_ocsp_staple(
    configured_version: &str,
    ssl_version: openssl::ssl::SslVersion,
    negotiated_version: &str,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let material = material(&format!("http://{}", listener.local_addr().unwrap()));
    let profile = material.server(ServerTlsOptions {
        backend: TlsBackend::OpenSsl,
        ocsp_stapling_secs: 1,
        reload_interval_secs: 60,
        parameters: TlsParameters {
            min_version: configured_version.into(),
            max_version: configured_version.into(),
            ..Default::default()
        },
        ..Default::default()
    });
    let context = super::super::super::OpenSslServerContext::build(&profile, None).unwrap();
    let runtime = std::sync::Arc::downgrade(&context.runtime);
    let response = OcspResponse::create(OcspResponseStatus::UNAUTHORIZED, None)
        .unwrap()
        .to_der()
        .unwrap();
    let mut request = tokio::time::timeout(Duration::from_secs(5), listener.accept())
        .await
        .unwrap()
        .unwrap()
        .0;
    read_request(&mut request).await;
    request
        .write_all(
            format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                response.len()
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    request.write_all(&response).await.unwrap();
    // Complete the close-delimited HTTP/1 response before waiting for the
    // background client to publish the staple. Dropping the socket directly
    // can race Hyper's EOF observation, especially in the second subcase.
    request.shutdown().await.unwrap();
    drop(request);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let prepared = context
                .runtime
                .current
                .read()
                .unwrap_or_else(|error| error.into_inner())
                .clone();
            if prepared.ocsp_targets[0].current().as_deref() == Some(response.as_slice()) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("OCSP staple was not published for TLS {configured_version}"));

    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let server = super::super::super::stream::OpenSslTlsStream::accept(&context, server_io);
    let client = async {
        let mut builder = SslContextBuilder::new(SslMethod::tls_client()).unwrap();
        builder.set_verify(SslVerifyMode::NONE);
        builder.set_min_proto_version(Some(ssl_version)).unwrap();
        builder.set_max_proto_version(Some(ssl_version)).unwrap();
        let mut ssl = Ssl::new(&builder.build()).unwrap();
        ssl.set_hostname("ocsp.test").unwrap();
        ssl.set_status_type(StatusType::OCSP).unwrap();
        let mut stream = tokio_openssl::SslStream::new(ssl, client_io).unwrap();
        Pin::new(&mut stream).connect().await.unwrap();
        assert_eq!(stream.ssl().version_str(), negotiated_version);
        assert_eq!(stream.ssl().ocsp_status(), Some(response.as_slice()));
        Ok::<_, io::Error>(())
    };
    let (server, client) = tokio::join!(server, client);
    server.unwrap();
    client.unwrap();

    drop(context);
    assert!(runtime.upgrade().is_none());
}

#[tokio::test]
async fn openssl_automatic_ocsp_refresh_staples_tls12_and_tls13_and_stops_on_drop() {
    for (configured, ssl, negotiated) in [
        ("1.2", openssl::ssl::SslVersion::TLS1_2, "TLSv1.2"),
        ("1.3", openssl::ssl::SslVersion::TLS1_3, "TLSv1.3"),
    ] {
        assert_automatic_ocsp_staple(configured, ssl, negotiated).await;
    }
}

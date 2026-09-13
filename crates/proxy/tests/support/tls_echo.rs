use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_rustls::{TlsAcceptor, TlsConnector};

pub async fn spawn_tls_echo(
    port: u16,
    payload_len: usize,
) -> (
    tokio::task::JoinHandle<()>,
    rustls::pki_types::CertificateDer<'static>,
) {
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
        .expect("generate TLS echo certificate");
    let cert = certified.cert.der().clone();
    let key = rustls::pki_types::PrivateKeyDer::from(rustls::pki_types::PrivatePkcs8KeyDer::from(
        certified.signing_key.serialize_der(),
    ));
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert.clone()], key)
        .expect("build TLS echo config");
    let acceptor = TlsAcceptor::from(Arc::new(config));
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
            .await
            .expect("bind TLS echo");
        let _ = ready_tx.send(());
        let (stream, _) = listener.accept().await.expect("accept TLS echo");
        let mut stream = acceptor.accept(stream).await.expect("accept TLS handshake");
        let mut payload = vec![0_u8; payload_len];
        stream
            .read_exact(&mut payload)
            .await
            .expect("read TLS payload");
        stream.write_all(&payload).await.expect("write TLS payload");
    });
    ready_rx.await.expect("TLS echo ready");
    (task, cert)
}

pub async fn socks5_tls_echo(
    proxy_port: u16,
    target_port: u16,
    payload: &[u8],
    cert: rustls::pki_types::CertificateDer<'static>,
) -> std::io::Result<Vec<u8>> {
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", proxy_port)).await?;
    stream.write_all(&[0x05, 0x01, 0x00]).await?;
    let mut auth = [0_u8; 2];
    stream.read_exact(&mut auth).await?;
    if auth != [0x05, 0x00] {
        return Err(std::io::Error::other("SOCKS authentication failed"));
    }
    let mut request = vec![0x05, 0x01, 0x00, 0x01, 127, 0, 0, 1];
    request.extend_from_slice(&target_port.to_be_bytes());
    stream.write_all(&request).await?;
    let mut response = [0_u8; 10];
    stream.read_exact(&mut response).await?;
    if response[1] != 0 {
        return Err(std::io::Error::other("SOCKS connect failed"));
    }

    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(cert)
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let server_name = rustls::pki_types::ServerName::try_from("localhost")
        .expect("valid TLS server name")
        .to_owned();
    let mut stream = connector.connect(server_name, stream).await?;
    stream.write_all(payload).await?;
    let mut echoed = vec![0_u8; payload.len()];
    stream.read_exact(&mut echoed).await?;
    Ok(echoed)
}

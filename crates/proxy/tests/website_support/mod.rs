use super::support::{free_port, free_udp_port, interop::TempMaterial};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
pub struct Site {
    _material: TempMaterial,
    cert: rcgen::CertifiedKey<rcgen::KeyPair>,
    cert_path: std::path::PathBuf,
    key_path: std::path::PathBuf,
    pub http: u16,
    pub https: u16,
    pub quic: u16,
}
impl Site {
    pub fn new() -> Self {
        let material = TempMaterial::new("hy2-website");
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert_path = material.path("cert.pem");
        let key_path = material.path("key.pem");
        std::fs::write(&cert_path, cert.cert.pem()).unwrap();
        std::fs::write(&key_path, cert.signing_key.serialize_pem()).unwrap();
        Self {
            _material: material,
            cert,
            cert_path,
            key_path,
            http: free_port(),
            https: free_port(),
            quic: free_udp_port(),
        }
    }
    pub fn config(&self, redirect: bool) -> serde_json::Value {
        serde_json::json!({"inbounds":[{"tag":"hy","listen":{"address":"127.0.0.1","port":self.quic},
            "protocol":{"type":"hysteria2","password":"test-password","cert_path":self.cert_path,"key_path":self.key_path,
                "masquerade":{"type":"string","content":"website","http":{"address":"127.0.0.1","port":self.http},
                "https":{"address":"127.0.0.1","port":self.https},"force_https":redirect}}}],
            "route":{"rules":[],"final":{"type":"direct"}}})
    }
    pub async fn tls(&self, alpn: &[u8]) -> tokio_rustls::client::TlsStream<TcpStream> {
        let mut roots = rustls::RootCertStore::empty();
        roots.add(self.cert.cert.der().clone()).unwrap();
        let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
        tls.alpn_protocols = vec![alpn.to_vec()];
        let socket = TcpStream::connect(("127.0.0.1", self.https)).await.unwrap();
        let stream = tokio_rustls::TlsConnector::from(Arc::new(tls))
            .connect("localhost".try_into().unwrap(), socket)
            .await
            .unwrap();
        assert_eq!(stream.get_ref().1.alpn_protocol(), Some(alpn));
        stream
    }
    pub async fn http2(&self) -> (u16, String, Vec<u8>) {
        let (mut sender, connection) = h2::client::handshake(self.tls(b"h2").await).await.unwrap();
        let task = tokio::spawn(connection);
        let request = http_uri::Request::builder()
            .uri("https://localhost/auth")
            .header("hysteria-auth", "test-password")
            .body(())
            .unwrap();
        let (response, _) = sender.send_request(request, true).unwrap();
        let response = response.await.unwrap();
        let status = response.status().as_u16();
        let alt = response.headers()["alt-svc"].to_str().unwrap().to_owned();
        let mut body = response.into_body();
        let mut data = Vec::new();
        while let Some(chunk) = body.data().await {
            data.extend(chunk.unwrap());
        }
        drop(body);
        drop(sender);
        task.abort();
        (status, alt, data)
    }
}
pub async fn http1<S: AsyncRead + AsyncWrite + Unpin>(socket: S) -> String {
    http1_host(socket, "localhost:80").await
}
pub async fn http1_host<S: AsyncRead + AsyncWrite + Unpin>(mut socket: S, host: &str) -> String {
    socket.write_all(format!("GET /auth HTTP/1.1\r\nHost: {host}\r\nHysteria-Auth: test-password\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
    let mut response = String::new();
    socket.read_to_string(&mut response).await.unwrap();
    response
}

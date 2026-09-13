#![cfg(all(feature = "vless", feature = "socks5", unix))]
mod support;
use serde_json::json;
use std::{path::PathBuf, sync::Arc};
use support::interop::{TempMaterial, XrayProcess};
use support::{free_port, spawn_engine, wait_for_listener};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use zero_config::RuntimeConfig;
use zero_proxy::Proxy;

struct SocketFile(PathBuf);
impl Drop for SocketFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
async fn receive<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    version: u8,
    path: &str,
    port: u16,
) {
    if version == 1 {
        let mut line = Vec::new();
        while !line.ends_with(b"\r\n") {
            line.push(stream.read_u8().await.unwrap());
            assert!(line.len() < 108);
        }
        let line = String::from_utf8(line).unwrap();
        let parts: Vec<_> = line.split_whitespace().collect();
        assert_eq!(&parts[..4], &["PROXY", "TCP4", "127.0.0.1", "127.0.0.1"]);
        assert_ne!(parts[4].parse::<u16>().unwrap(), 0);
        assert_eq!(parts[5].parse::<u16>().unwrap(), port);
    } else if version == 2 {
        let mut header = [0; 28];
        stream.read_exact(&mut header).await.unwrap();
        assert_eq!(&header[..16], b"\r\n\r\n\0\r\nQUIT\n\x21\x11\x00\x0c");
        assert_eq!(&header[16..24], &[127, 0, 0, 1, 127, 0, 0, 1]);
        assert_ne!(u16::from_be_bytes([header[24], header[25]]), 0);
        assert_eq!(u16::from_be_bytes([header[26], header[27]]), port);
    }
    let request = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n");
    let mut bytes = vec![0; request.len()];
    stream.read_exact(&mut bytes).await.unwrap();
    assert_eq!(bytes, request.as_bytes());
    stream.write_all(path.as_bytes()).await.unwrap();
    stream.flush().await.unwrap();
}
async fn run(official: bool) {
    let material = TempMaterial::new("fallback");
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into(), "www.localhost".into()])
        .unwrap();
    let cert_path = material.path("cert.pem");
    let key_path = material.path("key.pem");
    std::fs::write(&cert_path, cert.cert.pem()).unwrap();
    std::fs::write(&key_path, cert.signing_key.serialize_pem()).unwrap();
    let port = free_port();
    let default_port = free_port();
    let app_port = free_port();
    let socket_file = SocketFile(PathBuf::from(format!(
        "/tmp/zero-fb-{}-{port}.sock",
        std::process::id()
    )));
    let default = tokio::net::TcpListener::bind(("127.0.0.1", default_port))
        .await
        .unwrap();
    let app = tokio::net::TcpListener::bind(("127.0.0.1", app_port))
        .await
        .unwrap();
    let unix = tokio::net::UnixListener::bind(&socket_file.0).unwrap();
    let targets = tokio::spawn(async move {
        let (a, b, c) = tokio::join!(
            async { receive(default.accept().await.unwrap().0, 0, "/default", port).await },
            async { receive(app.accept().await.unwrap().0, 1, "/app?query=1", port).await },
            async { receive(unix.accept().await.unwrap().0, 2, "/unix", port).await }
        );
        (a, b, c)
    });
    let rules = json!([
        {"destination":{"type":"tcp","server":"127.0.0.1","port":default_port}},
        {"alpn":"http/1.1","destination":{"type":"tcp","server":"127.0.0.1","port":default_port}},
        {"name":"localhost","alpn":"http/1.1","path":"/app","destination":{"type":"tcp","server":"127.0.0.1","port":app_port},"proxy_protocol":1},
        {"path":"/unix","destination":{"type":"unix","path":socket_file.0},"proxy_protocol":2}]);
    let (zero, mut xray) = if official {
        let binary = std::env::var("XRAY_BIN").expect("official v26.3.27 binary required");
        let version = std::process::Command::new(&binary)
            .arg("version")
            .output()
            .unwrap();
        let version = String::from_utf8_lossy(&version.stdout);
        assert!(version.contains("26.3.27") && version.contains("d2758a0"));
        let config = json!({"log":{"loglevel":"debug"},"inbounds":[{"listen":"127.0.0.1","port":port,"protocol":"vless","settings":{"clients":[{"id":"fallback"}],"decryption":"none","fallbacks":[{"dest":format!("127.0.0.1:{default_port}")},{"alpn":"http/1.1","dest":format!("127.0.0.1:{default_port}")},{"name":"localhost","alpn":"http/1.1","path":"/app","dest":format!("127.0.0.1:{app_port}"),"xver":1},{"path":"/unix","dest":socket_file.0,"xver":2}]},"streamSettings":{"network":"raw","security":"tls","tlsSettings":{"alpn":["http/1.1"],"certificates":[{"certificateFile":cert_path,"keyFile":key_path}]}}}],"outbounds":[{"protocol":"freedom"}]});
        let path = material.path("xray.json");
        std::fs::write(&path, config.to_string()).unwrap();
        (None, Some(XrayProcess::start(binary, &path, &material)))
    } else {
        let config = json!({"inbounds":[{"tag":"in","listen":{"address":"127.0.0.1","port":port},"protocol":{"type":"vless","users":[{"id":"fallback"}],"tls":{"cert_path":cert_path,"key_path":key_path,"alpn":["http/1.1"]},"fallback":{"rules":rules}}}],"route":{"rules":[],"final":{"type":"direct"}}});
        (
            Some(spawn_engine(
                Proxy::new(RuntimeConfig::parse(&config.to_string()).unwrap()).unwrap(),
            )),
            None,
        )
    };
    wait_for_listener(port).await;
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let mut config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
    let result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        for path in ["/default", "/app?query=1", "/unix"] {
            let socket = tokio::net::TcpStream::connect(("127.0.0.1", port))
                .await
                .unwrap();
            let mut stream = connector
                .connect("www.localhost".try_into().unwrap(), socket)
                .await
                .unwrap();
            stream
                .write_all(format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n").as_bytes())
                .await
                .unwrap();
            let mut bytes = vec![0; path.len()];
            stream.read_exact(&mut bytes).await.unwrap();
            assert_eq!(bytes, path.as_bytes());
        }
        targets.await.unwrap();
    })
    .await;
    if let Some(xray) = &mut xray {
        xray.kill();
    }
    if let Some(zero) = zero {
        zero.shutdown().await.unwrap();
    }
    assert!(
        result.is_ok(),
        "official={official}: {}",
        xray.map(|x| x.logs()).unwrap_or_default()
    );
}
#[tokio::test]
async fn fallback_rules_select_sni_alpn_path_and_replay_to_tcp_unix_with_proxy_headers() {
    run(false).await;
}
#[tokio::test]
#[ignore = "requires official XRAY_BIN v26.3.27"]
async fn official_fallback_uses_the_same_selection_and_proxy_wire() {
    run(true).await;
}

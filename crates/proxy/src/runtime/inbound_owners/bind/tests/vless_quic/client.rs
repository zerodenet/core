use super::*;
pub(super) async fn open(
    client: &Client,
    address: std::net::SocketAddr,
    carrier: &str,
) -> (Option<quinn::Connection>, TcpRelayStream) {
    if let Client::Mkcp(factory, settings) = client {
        let stream = zero_transport::mkcp::connect(
            &address.ip().to_string(),
            address.port(),
            *settings,
            factory,
        )
        .await
        .unwrap();
        return (None, TcpRelayStream::new(stream));
    }
    let Client::Quic(client) = client else {
        unreachable!()
    };
    let connection = client.connect(address, "localhost").unwrap().await.unwrap();
    let stream = if let Some(mode) = carrier.strip_prefix("h3-") {
        let profile: zero_config::SplitHttpConfig =
            serde_json::from_value(serde_json::json!({"path":"/scope/","mode":mode})).unwrap();
        TcpRelayStream::new(
            zero_transport::split_http::connect_xhttp_h3(connection.clone(), &profile)
                .await
                .unwrap(),
        )
    } else if carrier.starts_with("hysteria") {
        let profile = zero_transport::hysteria::Profile::new("original").unwrap();
        let authenticated =
            zero_transport::hysteria::Client::authenticate(connection.clone(), &profile)
                .await
                .unwrap();
        TcpRelayStream::new(authenticated.open().await.unwrap())
    } else {
        let (send, recv) = connection.open_bi().await.unwrap();
        TcpRelayStream::new(Raw {
            send,
            recv,
            peer: address,
        })
    };
    (Some(connection), stream)
}
pub(super) enum Client {
    Quic(quinn::Endpoint),
    Mkcp(
        zero_transport::OutboundDatagramSocketFactory,
        zero_transport::mkcp::Settings,
    ),
}
pub(super) fn new(
    carrier: &str,
    masks: Vec<zero_transport::finalmask::udp::Mask>,
    mtu: u32,
    cert: &std::path::Path,
) -> Client {
    if carrier.starts_with("mkcp") {
        return Client::Mkcp(
            zero_transport::OutboundDatagramSocketFactory::new(Default::default())
                .with_final_mask(zero_transport::finalmask::Profile::from_udp(masks).unwrap()),
            zero_transport::mkcp::Settings {
                mtu,
                ..Default::default()
            },
        );
    }
    let raw = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    raw.set_nonblocking(true).unwrap();
    let socket = quinn::Runtime::wrap_udp_socket(&quinn::TokioRuntime, raw).unwrap();
    let socket = zero_transport::finalmask::Socket::wrap(socket, &masks, false).unwrap();
    let mut endpoint = quinn::Endpoint::new_with_abstract_socket(
        Default::default(),
        None,
        socket,
        std::sync::Arc::new(quinn::TokioRuntime),
    )
    .unwrap();
    endpoint.set_default_client_config(
        zero_transport::quic::client_config_with_ca(
            false,
            None,
            &[b"h3".to_vec()],
            Some(65536),
            Some(cert),
        )
        .unwrap(),
    );
    Client::Quic(endpoint)
}
pub(super) fn masks(
    carrier: &str,
) -> (serde_json::Value, Vec<zero_transport::finalmask::udp::Mask>) {
    use zero_transport::finalmask::{
        noise::{Item, Settings},
        tcp::Range,
        udp::Mask,
    };
    if carrier.ends_with("noise") {
        (
            serde_json::json!({"udp":[{"type":"noise","items":[{"packet":[78,79,73,83,69]}]}]}),
            vec![Mask::Noise(Settings {
                reset_seconds: Range::default(),
                items: vec![Item {
                    packet: vec![78, 79, 73, 83, 69],
                    random_length: Range::default(),
                    minimum_byte: 0,
                    maximum_byte: 255,
                    delay_ms: Range::default(),
                }],
            })],
        )
    } else if carrier.ends_with("xdns") {
        (
            serde_json::json!({"udp":[{"type":"xdns","domain":"t.example.com"}]}),
            vec![Mask::Xdns {
                domain: "t.example.com".into(),
            }],
        )
    } else if carrier.ends_with("xicmp") {
        (
            serde_json::json!({"udp":[{"type":"xicmp","ip":"127.0.0.1","id":54321}]}),
            vec![Mask::Xicmp {
                ip: "127.0.0.1".into(),
                id: 54321,
            }],
        )
    } else {
        (serde_json::json!({}), vec![])
    }
}

struct Raw {
    send: quinn::SendStream,
    recv: quinn::RecvStream,
    peer: std::net::SocketAddr,
}
impl tokio::io::AsyncRead for Raw {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        b: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        tokio::io::AsyncRead::poll_read(std::pin::Pin::new(&mut self.recv), cx, b)
    }
}
impl tokio::io::AsyncWrite for Raw {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        b: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        tokio::io::AsyncWrite::poll_write(std::pin::Pin::new(&mut self.send), cx, b)
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        tokio::io::AsyncWrite::poll_flush(std::pin::Pin::new(&mut self.send), cx)
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        tokio::io::AsyncWrite::poll_shutdown(std::pin::Pin::new(&mut self.send), cx)
    }
}
impl zero_traits::AsyncSocket for Raw {
    type Error = std::io::Error;
    async fn read(&mut self, b: &mut [u8]) -> std::io::Result<usize> {
        AsyncReadExt::read(self, b).await
    }
    async fn write_all(&mut self, b: &[u8]) -> std::io::Result<()> {
        AsyncWriteExt::write_all(self, b).await
    }
    async fn shutdown(&mut self) -> std::io::Result<()> {
        AsyncWriteExt::shutdown(self).await
    }
}
impl zero_platform_tokio::ClientStream for Raw {
    fn local_addr(&self) -> std::io::Result<std::net::SocketAddr> {
        Err(std::io::ErrorKind::Unsupported.into())
    }
    fn peer_addr(&self) -> std::io::Result<std::net::SocketAddr> {
        Ok(self.peer)
    }
}

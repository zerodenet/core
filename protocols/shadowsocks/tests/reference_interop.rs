#![cfg(all(feature = "runtime", feature = "blake3"))]
#[path = "support/reference.rs"]
mod reference;
use reference::*;
use shadowsocks::{
    udp::{ShadowsocksDatagramCodec, ShadowsocksInboundUdpCodec},
    CipherKind, ShadowsocksInboundProfile, ShadowsocksInboundTcpAcceptor,
    ShadowsocksTcpConnectConfig,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket},
    time::{timeout, Duration},
};
use zero_core::{Address, Network, ProtocolType, Session};
use zero_platform_tokio::TokioSocket;
use zero_traits::DatagramCodec;
fn target() -> Address {
    Address::Ipv4([127, 0, 0, 1])
}
fn args(method: &str, port: u16) -> Vec<String> {
    vec![
        "-s".into(),
        format!("127.0.0.1:{port}"),
        "-m".into(),
        method.into(),
        "-k".into(),
        password(method).into(),
        "-U".into(),
    ]
}

#[tokio::test]
#[ignore = "requires fixed official 1.21.2 binaries in SS_RUST_BIN_DIR"]
async fn every_release_method_zero_to_official_tcp_and_udp() {
    for method in methods() {
        eprintln!(
            "{}: {method}",
            std::thread::current().name().unwrap_or("matrix")
        );
        timeout(Duration::from_secs(30), async {
            let port = free_port();
            let _server = launch("ssserver", &args(method, port));
            ready(port).await;
            let echo = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let echo_port = echo.local_addr().unwrap().port();
            let payload = vec![0x42; 4096];
            let expected = payload.clone();
            let echo = tokio::spawn(async move {
                let (mut stream, _) = echo.accept().await.unwrap();
                let mut data = vec![0; expected.len()];
                stream.read_exact(&mut data).await.unwrap();
                assert_eq!(data, expected);
                stream.write_all(&data).await.unwrap();
            });
            let config =
                ShadowsocksTcpConnectConfig::from_config(method, password(method)).unwrap();
            let mut socket =
                TokioSocket::new(TcpStream::connect(("127.0.0.1", port)).await.unwrap());
            let session = Session::new(
                0,
                target(),
                echo_port,
                Network::Tcp,
                ProtocolType::new("shadowsocks"),
            );
            let established = config
                .establish_tcp_session(&mut socket, &session)
                .await
                .unwrap();
            let mut stream = config.wrap_outbound_stream(socket, established);
            stream.write_all(&payload).await.unwrap();
            stream.flush().await.unwrap();
            let mut reply = vec![0; payload.len()];
            stream.read_exact(&mut reply).await.unwrap();
            assert_eq!(reply, payload);
            stream.shutdown().await.unwrap();
            echo.await.unwrap();
            let upstream = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let codec = ShadowsocksDatagramCodec::new(
                CipherKind::from_str(method).unwrap(),
                password(method),
            );
            let packet = codec
                .encode(
                    &target(),
                    upstream.local_addr().unwrap().port(),
                    b"reference-udp",
                )
                .unwrap();
            client.send_to(&packet, ("127.0.0.1", port)).await.unwrap();
            let mut bytes = vec![0; 65535];
            let (size, peer) = upstream.recv_from(&mut bytes).await.unwrap();
            assert_eq!(&bytes[..size], b"reference-udp");
            upstream.send_to(&bytes[..size], peer).await.unwrap();
            let (size, _) = client.recv_from(&mut bytes).await.unwrap();
            assert_eq!(codec.decode(&bytes[..size]).unwrap().2, b"reference-udp");
        })
        .await
        .unwrap_or_else(|_| panic!("Zero to official {method} timed out"));
    }
}

#[tokio::test]
#[ignore = "requires fixed official 1.21.2 binaries in SS_RUST_BIN_DIR"]
async fn every_release_method_official_to_zero_tcp_and_udp() {
    for method in methods() {
        eprintln!(
            "{}: {method}",
            std::thread::current().name().unwrap_or("matrix")
        );
        timeout(Duration::from_secs(30), async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let udp = UdpSocket::bind(("127.0.0.1", port)).await.unwrap();
            let local = free_port();
            let mut options = args(method, port);
            options.extend(["-b".into(), format!("127.0.0.1:{local}")]);
            let _client = launch("sslocal", &options);
            ready(local).await;
            let profile =
                ShadowsocksInboundProfile::from_config_cipher_password(method, password(method))
                    .unwrap();
            let tcp = tokio::spawn(async move {
                let (socket, _) = listener.accept().await.unwrap();
                let (session, mut stream) = ShadowsocksInboundTcpAcceptor::new(profile)
                    .accept_stream(TokioSocket::new(socket))
                    .await
                    .unwrap();
                assert_eq!(session.target, target());
                assert_eq!(session.port, 9000);
                let mut data = vec![0; 4096];
                stream.read_exact(&mut data).await.unwrap();
                stream.write_all(&data).await.unwrap();
                stream.shutdown().await.unwrap();
            });
            let (mut stream, _) = socks(local, 1, 9000).await;
            let payload = vec![0x25; 4096];
            stream.write_all(&payload).await.unwrap();
            let mut reply = vec![0; 4096];
            stream.read_exact(&mut reply).await.unwrap();
            assert_eq!(reply, payload);
            stream.shutdown().await.unwrap();
            tcp.await.unwrap();
            let (_control, relay) = socks(local, 3, 0).await;
            let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let plain = shadowsocks::build_target_data(&target(), 9001, b"official-udp").unwrap();
            socket
                .send_to(&[vec![0, 0, 0], plain].concat(), ("127.0.0.1", relay))
                .await
                .unwrap();
            let mut bytes = vec![0; 65535];
            let (size, peer) = udp.recv_from(&mut bytes).await.unwrap();
            let mut codec = ShadowsocksInboundUdpCodec::new(
                CipherKind::from_str(method).unwrap(),
                password(method).as_bytes(),
            );
            let request = codec.decode_request(&bytes[..size]).unwrap();
            let (address, port, payload, session) = request.into_parts();
            assert_eq!(payload, b"official-udp");
            let response = codec
                .encode_response(session, &address, port, &payload)
                .unwrap();
            udp.send_to(&response, peer).await.unwrap();
            let (size, _) = socket.recv_from(&mut bytes).await.unwrap();
            let (_, _, offset) = shadowsocks::parse_target_data(&bytes[3..size]).unwrap();
            assert_eq!(&bytes[3 + offset..size], b"official-udp");
        })
        .await
        .unwrap_or_else(|_| panic!("official to Zero {method} timed out"));
    }
}

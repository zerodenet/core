#![cfg(all(feature = "crypto", feature = "blake3"))]

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use shadowsocks::transport::ShadowsocksInboundUserRef;
use shadowsocks::udp::{ShadowsocksDatagramCodec, ShadowsocksInboundUdpResponder};
use shadowsocks::{
    tcp_connect_config_from_config, CipherKind, ShadowsocksInboundProfile,
    ShadowsocksInboundTcpAcceptor,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use zero_core::{Address, Network, ProtocolType, Session};
use zero_traits::{AsyncSocket, DatagramCodec};

#[cfg(windows)]
const CREATE_NO_WINDOW: u32 = 0x0800_0000;

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct TempConfig(PathBuf);

impl Drop for TempConfig {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

struct NetSocket(TcpStream);

impl AsyncSocket for NetSocket {
    type Error = std::io::Error;

    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        AsyncReadExt::read(&mut self.0, buf).await
    }

    async fn write_all(&mut self, buf: &[u8]) -> Result<(), Self::Error> {
        AsyncWriteExt::write_all(&mut self.0, buf).await
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        AsyncWriteExt::shutdown(&mut self.0).await
    }
}

impl AsyncRead for NetSocket {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl AsyncWrite for NetSocket {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

fn methods() -> [(CipherKind, &'static str, &'static str, &'static str); 2] {
    [
        (
            CipherKind::Blake3Aes128Gcm,
            "2022-blake3-aes-128-gcm",
            "MDEyMzQ1Njc4OWFiY2RlZg==",
            "ZmVkY2JhOTg3NjU0MzIxMA==",
        ),
        (
            CipherKind::Blake3Aes256Gcm,
            "2022-blake3-aes-256-gcm",
            "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=",
            "ZmVkY2JhOTg3NjU0MzIxMGZlZGNiYTk4NzY1NDMyMTA=",
        ),
    ]
}

fn managed_user<'a>(password: &'a str) -> ShadowsocksInboundUserRef<'a> {
    ShadowsocksInboundUserRef {
        password,
        principal_key: Some("external-sip023"),
        up_bps: None,
        down_bps: None,
        device_limit: None,
        quota_remaining_bytes: None,
        policy_revision: Some(1),
    }
}

fn external_command(binary: &str) -> Command {
    let mut command = Command::new(binary);
    if std::env::var_os("ZERO_EXTERNAL_INTEROP_LOG").is_some() {
        command.stdout(Stdio::inherit()).stderr(Stdio::inherit());
    } else {
        command.stdout(Stdio::null()).stderr(Stdio::null());
    }
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(CREATE_NO_WINDOW);
    }
    command
}

fn require_binary(binary: &str) {
    let status = external_command(binary)
        .arg("--version")
        .status()
        .unwrap_or_else(|error| panic!("{binary} is required on PATH: {error}"));
    assert!(status.success(), "{binary} --version failed");
}

async fn connect_retry(address: SocketAddr) -> TcpStream {
    let mut last_error = None;
    for _ in 0..50 {
        match TcpStream::connect(address).await {
            Ok(stream) => return stream,
            Err(error) => last_error = Some(error),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("external process did not listen on {address}: {last_error:?}");
}

async fn socks5_connect(stream: &mut TcpStream, target: SocketAddr) {
    stream.write_all(&[5, 1, 0]).await.unwrap();
    let mut method = [0_u8; 2];
    stream.read_exact(&mut method).await.unwrap();
    assert_eq!(method, [5, 0]);

    let IpAddr::V4(ip) = target.ip() else {
        panic!("test target must be IPv4");
    };
    let mut request = vec![5, 1, 0, 1];
    request.extend_from_slice(&ip.octets());
    request.extend_from_slice(&target.port().to_be_bytes());
    stream.write_all(&request).await.unwrap();

    let mut head = [0_u8; 4];
    stream.read_exact(&mut head).await.unwrap();
    assert_eq!(head[0], 5);
    assert_eq!(head[1], 0, "SOCKS5 CONNECT failed with reply {}", head[1]);
    let remaining = match head[3] {
        1 => 6,
        4 => 18,
        3 => {
            let length = stream.read_u8().await.unwrap() as usize;
            length + 2
        }
        atyp => panic!("unexpected SOCKS5 address type {atyp}"),
    };
    let mut bound = vec![0_u8; remaining];
    stream.read_exact(&mut bound).await.unwrap();
}

fn temp_config_path(method: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "zero-shadowsocks-{method}-{}-{nonce}.json",
        std::process::id()
    ))
}

fn spawn_ssserver(
    address: SocketAddr,
    method: &str,
    identity_password: &str,
    user_password: &str,
    mode: &str,
) -> (ChildGuard, TempConfig) {
    let config_path = temp_config_path(method);
    let config = format!(
        "{{\n  \"servers\": [{{\n    \"address\": \"127.0.0.1\",\n    \"port\": {},\n    \"method\": \"{}\",\n    \"password\": \"{}\",\n    \"mode\": \"{}\",\n    \"users\": [{{ \"name\": \"zero-interop\", \"password\": \"{}\" }}]\n  }}]\n}}\n",
        address.port(), method, identity_password, mode, user_password
    );
    std::fs::write(&config_path, config).unwrap();
    let child = external_command("ssserver")
        .args(["-c", config_path.to_str().unwrap(), "-vvv"])
        .spawn()
        .expect("start ssserver");
    (ChildGuard(child), TempConfig(config_path))
}

#[tokio::test]
#[ignore = "requires shadowsocks-rust sslocal 1.24+ on PATH"]
async fn shadowsocks_rust_client_reaches_zero_sip023_inbound() {
    require_binary("sslocal");

    for (_cipher, method, identity_password, user_password) in methods() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let zero_address = listener.local_addr().unwrap();
        let local_probe = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let local_address = local_probe.local_addr().unwrap();
        drop(local_probe);

        let profile = ShadowsocksInboundProfile::from_config_users_with_identity(
            method,
            Some(identity_password),
            [managed_user(user_password)],
        )
        .unwrap();
        let acceptor = ShadowsocksInboundTcpAcceptor::new(profile);
        let target = SocketAddr::from((Ipv4Addr::LOCALHOST, 4242));
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (session, mut stream) = acceptor.accept_stream(NetSocket(stream)).await.unwrap();
            assert_eq!(session.target, Address::Ipv4([127, 0, 0, 1]));
            assert_eq!(session.port, 4242);
            let mut payload = [0_u8; 20];
            stream.read_exact(&mut payload).await.unwrap();
            stream.write_all(&payload).await.unwrap();
            stream.flush().await.unwrap();
        });

        let password_chain = format!("{identity_password}:{user_password}");
        let child = external_command("sslocal")
            .args([
                "-b",
                &local_address.to_string(),
                "-s",
                &zero_address.to_string(),
                "-m",
                method,
                "-k",
                &password_chain,
            ])
            .spawn()
            .expect("start sslocal");
        let _child = ChildGuard(child);

        let mut client = connect_retry(local_address).await;
        socks5_connect(&mut client, target).await;
        client.write_all(b"external-to-zero-eih").await.unwrap();
        let mut echoed = [0_u8; 20];
        client.read_exact(&mut echoed).await.unwrap();
        assert_eq!(&echoed, b"external-to-zero-eih");
        server.await.unwrap();
    }
}

#[tokio::test]
#[ignore = "requires shadowsocks-rust ssserver 1.24+ on PATH"]
async fn zero_sip023_outbound_reaches_shadowsocks_rust_server() {
    require_binary("ssserver");

    for (_cipher, method, identity_password, user_password) in methods() {
        let ss_probe = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let ss_address = ss_probe.local_addr().unwrap();
        drop(ss_probe);
        let target_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let target_address = target_listener.local_addr().unwrap();
        let target = tokio::spawn(async move {
            let (mut stream, _) = target_listener.accept().await.unwrap();
            let mut payload = [0_u8; 20];
            stream.read_exact(&mut payload).await.unwrap();
            stream.write_all(&payload).await.unwrap();
            stream.flush().await.unwrap();
        });

        let (_child, _config) = spawn_ssserver(
            ss_address,
            method,
            identity_password,
            user_password,
            "tcp_only",
        );

        let stream = connect_retry(ss_address).await;
        let mut socket = NetSocket(stream);
        let password_chain = format!("{identity_password}:{user_password}");
        let connect = tcp_connect_config_from_config(method, &password_chain).unwrap();
        let session = Session::new(
            0,
            Address::Ipv4([127, 0, 0, 1]),
            target_address.port(),
            Network::Tcp,
            ProtocolType::new("shadowsocks"),
        );
        let outbound_session = connect
            .establish_tcp_session(&mut socket, &session)
            .await
            .unwrap();
        let mut stream = connect.wrap_outbound_stream(socket, outbound_session);
        stream.write_all(b"zero-to-external-eih").await.unwrap();
        stream.flush().await.unwrap();
        let mut echoed = [0_u8; 20];
        stream.read_exact(&mut echoed).await.unwrap();
        assert_eq!(&echoed, b"zero-to-external-eih");
        target.await.unwrap();
    }
}

#[tokio::test]
#[ignore = "requires shadowsocks-rust sslocal 1.24+ on PATH"]
async fn shadowsocks_rust_udp_client_reaches_zero_sip023_inbound() {
    require_binary("sslocal");

    for (_cipher, method, identity_password, user_password) in methods() {
        let zero_socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let zero_address = zero_socket.local_addr().unwrap();
        let local_probe = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let local_address = local_probe.local_addr().unwrap();
        drop(local_probe);
        let profile = ShadowsocksInboundProfile::from_config_users_with_identity(
            method,
            Some(identity_password),
            [managed_user(user_password)],
        )
        .unwrap();
        let mut responder = ShadowsocksInboundUdpResponder::from_profile(profile);
        let password_chain = format!("{identity_password}:{user_password}");
        let child = external_command("sslocal")
            .args([
                "-u",
                "--protocol",
                "tunnel",
                "-b",
                &local_address.to_string(),
                "-f",
                "127.0.0.1:4242",
                "-s",
                &zero_address.to_string(),
                "-m",
                method,
                "-k",
                &password_chain,
            ])
            .spawn()
            .expect("start sslocal UDP tunnel");
        let _child = ChildGuard(child);
        tokio::time::sleep(Duration::from_millis(500)).await;

        let client = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        client
            .send_to(b"external-udp-to-zero", local_address)
            .await
            .unwrap();
        let dispatch = tokio::time::timeout(
            Duration::from_secs(5),
            responder.read_inbound_dispatch_from_socket_tokio(&zero_socket),
        )
        .await
        .expect("receive EIH UDP request")
        .unwrap();
        assert_eq!(dispatch.target(), &Address::Ipv4([127, 0, 0, 1]));
        assert_eq!(dispatch.port(), 4242);
        assert_eq!(dispatch.payload(), b"external-udp-to-zero");
        responder.record_pending_dispatch_success(1, dispatch.client_session_id());
        responder
            .send_response_for_target_proxy_session_to_client_tokio(
                &zero_socket,
                Some(1),
                dispatch.target(),
                dispatch.port(),
                b"zero-udp-to-external",
            )
            .await
            .unwrap()
            .expect("encoded EIH response");

        let mut response = [0_u8; 64];
        let (n, _) = tokio::time::timeout(Duration::from_secs(5), client.recv_from(&mut response))
            .await
            .expect("receive sslocal UDP response")
            .unwrap();
        assert_eq!(&response[..n], b"zero-udp-to-external");
    }
}

#[tokio::test]
#[ignore = "requires shadowsocks-rust ssserver 1.24+ on PATH"]
async fn zero_sip023_udp_outbound_reaches_shadowsocks_rust_server() {
    require_binary("ssserver");

    for (cipher, method, identity_password, user_password) in methods() {
        let ss_probe = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let ss_address = ss_probe.local_addr().unwrap();
        drop(ss_probe);
        let target_socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let target_address = target_socket.local_addr().unwrap();
        let target = tokio::spawn(async move {
            let mut request = [0_u8; 64];
            let (n, peer) = target_socket.recv_from(&mut request).await.unwrap();
            assert_eq!(&request[..n], b"zero-udp-to-external");
            target_socket
                .send_to(b"external-udp-to-zero", peer)
                .await
                .unwrap();
        });
        let (_child, _config) = spawn_ssserver(
            ss_address,
            method,
            identity_password,
            user_password,
            "tcp_and_udp",
        );
        // The UDP socket can lag process creation on Windows. The server
        // binds its TCP and UDP listeners during the same startup path, so a
        // successful TCP connect is a deterministic readiness barrier and
        // avoids a one-shot ICMP port-unreachable race below.
        drop(connect_retry(ss_address).await);

        let password_chain = format!("{identity_password}:{user_password}");
        let codec = ShadowsocksDatagramCodec {
            cipher,
            password: password_chain.into_bytes(),
        };
        let request = codec
            .encode(
                &Address::Ipv4([127, 0, 0, 1]),
                target_address.port(),
                b"zero-udp-to-external",
            )
            .unwrap();
        let client = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        client.send_to(&request, ss_address).await.unwrap();
        let mut response = [0_u8; 65_535];
        let (n, _) = tokio::time::timeout(Duration::from_secs(5), client.recv_from(&mut response))
            .await
            .expect("receive ssserver UDP response")
            .unwrap();
        let (source, port, payload) = codec.decode(&response[..n]).unwrap();
        assert_eq!(source, Address::Ipv4([127, 0, 0, 1]));
        assert_eq!(port, target_address.port());
        assert_eq!(payload, b"external-udp-to-zero");
        target.await.unwrap();
    }
}

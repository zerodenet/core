use socks5::{
    ConfiguredSocks5PasswordAuth, ConfiguredSocks5User, Socks5Command, Socks5CommandFacts,
    Socks5InboundTcpAcceptor, Socks5Request,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_core::{request::Destination, Address, SessionAuth};
use zero_platform_tokio::TokioSocket;

fn acceptor() -> Socks5InboundTcpAcceptor {
    Socks5InboundTcpAcceptor::new(ConfiguredSocks5PasswordAuth::from_users(vec![
        ConfiguredSocks5User::new("user".into(), "secret".into(), None, Some(123), Some(456)),
    ]))
}

async fn client(address: std::net::SocketAddr, command: u8) -> (tokio::net::TcpStream, Vec<u8>) {
    let mut io = tokio::net::TcpStream::connect(address).await.unwrap();
    io.write_all(&[5, 1, 2]).await.unwrap();
    let mut methods = [0; 2];
    io.read_exact(&mut methods).await.unwrap();
    assert_eq!(methods, [5, 2]);
    io.write_all(b"\x01\x04user\x06secret").await.unwrap();
    let mut authenticated = [0; 2];
    io.read_exact(&mut authenticated).await.unwrap();
    assert_eq!(authenticated, [1, 0]);
    let request = match command {
        1 => b"\x05\x01\x00\x03\x0ctarget.local\x01\xbb".as_slice(),
        3 => b"\x05\x03\x00\x01\x00\x00\x00\x00\x00\x00".as_slice(),
        _ => unreachable!(),
    };
    // Pipelined bytes remain in the same carrier, beyond the exact request.
    let mut write = request.to_vec();
    write.extend_from_slice(b"tail");
    io.write_all(&write).await.unwrap();
    (io, [methods, authenticated].concat())
}

async fn facts(command: u8) -> Socks5CommandFacts {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        let mut io = TokioSocket::new(io);
        let acceptor = acceptor();
        let facts = acceptor.accept_command_facts(&mut io).await.unwrap();
        let mut tail = [0; 4];
        io.read_exact(&mut tail).await.unwrap();
        assert_eq!(tail, *b"tail");
        acceptor.send_success(&mut io).await.unwrap();
        facts
    });
    let (mut io, handshake) = client(address, command).await;
    assert_eq!(handshake, [5, 2, 1, 0]);
    let mut response = [0; 10];
    io.read_exact(&mut response).await.unwrap();
    assert_eq!(response, [5, 0, 0, 1, 0, 0, 0, 0, 0, 0]);
    server.await.unwrap()
}

fn authenticated() -> SessionAuth {
    let mut auth = SessionAuth::new("socks5");
    auth.up_bps = Some(123);
    auth.down_bps = Some(456);
    auth
}

#[tokio::test]
async fn authenticated_connect_facts_preserve_limits_without_a_business_identity() {
    let facts = facts(1).await;
    assert_eq!(
        facts.command,
        Socks5Command::Connect(Destination::new(
            Address::Domain("target.local".into()),
            443
        ))
    );
    // The configured absent key remains absent; no username-derived key.
    assert_eq!(facts.auth, Some(authenticated()));
}

#[tokio::test]
async fn authenticated_udp_command_retains_hint_and_authentication_without_inventing_target() {
    let facts = facts(3).await;
    let Socks5Command::UdpAssociate(request) = facts.command else {
        panic!("UDP association remains its own command");
    };
    assert_eq!(request.client_hint, Address::Ipv4([0; 4]));
    assert_eq!(request.client_port, 0);
    assert_eq!(facts.auth, Some(authenticated()));
}

#[tokio::test]
async fn legacy_connect_uses_same_verified_facts_and_wire() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        let mut io = TokioSocket::new(io);
        let acceptor = acceptor();
        let request = acceptor.accept_command(&mut io).await.unwrap();
        let mut tail = [0; 4];
        io.read_exact(&mut tail).await.unwrap();
        assert_eq!(tail, *b"tail");
        acceptor.send_success(&mut io).await.unwrap();
        request
    });
    let (mut io, handshake) = client(address, 1).await;
    assert_eq!(handshake, [5, 2, 1, 0]);
    let mut response = [0; 10];
    io.read_exact(&mut response).await.unwrap();
    assert_eq!(response, [5, 0, 0, 1, 0, 0, 0, 0, 0, 0]);
    let Socks5Request::Connect(session) = server.await.unwrap() else {
        panic!("legacy CONNECT remains a Session");
    };
    assert_eq!(session.id, 0); // Existing compatibility shape, not a native ID.
    assert_eq!(session.target, Address::Domain("target.local".into()));
    assert_eq!(session.port, 443);
    assert_eq!(session.auth, Some(authenticated()));
}

#[tokio::test]
async fn legacy_udp_preserves_its_authentication_provider_callback_behavior() {
    struct Auth;
    impl socks5::Socks5PasswordAuth for Auth {
        fn required(&self) -> bool {
            true
        }
        fn verify(&self, username: &str, password: &str) -> bool {
            username == "user" && password == "secret"
        }
        fn principal_key_for(&self, _: &str) -> Option<String> {
            panic!("legacy UDP did not request this policy projection")
        }
        fn rate_limit_for(&self, _: &str) -> (Option<u64>, Option<u64>) {
            panic!("legacy UDP did not request this policy projection")
        }
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        let mut io = TokioSocket::new(io);
        let protocol = socks5::Socks5Inbound;
        let request = protocol
            .accept_command_with_auth(&mut io, &Auth)
            .await
            .unwrap();
        let mut tail = [0; 4];
        io.read_exact(&mut tail).await.unwrap();
        assert_eq!(tail, *b"tail");
        protocol.send_success_response(&mut io).await.unwrap();
        request
    });
    let (mut io, _) = client(address, 3).await;
    let mut response = [0; 10];
    io.read_exact(&mut response).await.unwrap();
    assert_eq!(response, [5, 0, 0, 1, 0, 0, 0, 0, 0, 0]);
    assert!(matches!(
        server.await.unwrap(),
        Socks5Request::UdpAssociate(_)
    ));
}

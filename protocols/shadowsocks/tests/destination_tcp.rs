#![cfg(feature = "runtime")]
use crate::socket::Socket;
use shadowsocks::{
    ShadowsocksInboundProfile, ShadowsocksInboundTcpAcceptor, ShadowsocksTcpConnectConfig,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_core::{request::Destination, Address};

#[tokio::test]
async fn destination_only_tcp_framing_preserves_address_port_and_payload_without_session_identity()
{
    let methods = [
        ("none", "password"),
        ("aes-256-gcm", "password"),
        ("2022-blake3-aes-128-gcm", "MDEyMzQ1Njc4OWFiY2RlZg=="),
    ];
    for (method, password) in methods {
        if method.starts_with("2022-") && !cfg!(feature = "blake3") {
            continue;
        }
        for address in [
            Address::Domain("destination.example".into()),
            Address::Ipv4([203, 0, 113, 9]),
            Address::Ipv6([1; 16]),
        ] {
            let destination = Destination::new(address, 8443);
            let config = ShadowsocksTcpConnectConfig::from_config(method, password).unwrap();
            let mut socket = Socket::new(vec![]);
            socket.fragmented_io = true;
            let wire = socket.output.clone();
            let state = config
                .establish_tcp_to(&mut socket, &destination)
                .await
                .unwrap();
            let mut stream = config.wrap_outbound_stream(socket, state);
            stream.write_all(b"first").await.unwrap();
            stream.write_all(b"second").await.unwrap();
            stream.shutdown().await.unwrap();
            let mut socket = Socket::new(wire.lock().unwrap().clone());
            socket.fragment = true;
            socket.fragmented_io = true;
            let profile =
                ShadowsocksInboundProfile::from_config_cipher_password(method, password).unwrap();
            let (accepted, mut stream) = ShadowsocksInboundTcpAcceptor::new(profile)
                .accept_stream(socket)
                .await
                .unwrap();
            assert_eq!(accepted.target, destination.address, "{method}");
            assert_eq!(accepted.port, destination.port, "{method}");
            let mut payload = vec![];
            stream.read_to_end(&mut payload).await.unwrap();
            assert_eq!(payload, b"firstsecond", "{method}");
        }
    }
}

#![cfg(feature = "runtime")]
#[path = "support/socket.rs"]
mod socket;
use shadowsocks::udp::ShadowsocksDatagramCodec;
use shadowsocks::{
    CipherKind, ShadowsocksInboundProfile, ShadowsocksInboundTcpAcceptor,
    ShadowsocksTcpConnectConfig,
};
use shadowsocks_crypto::{v1::Cipher, CipherKind as ReferenceKind};
use socket::Socket;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_core::{Address, Network, ProtocolType, Session};
use zero_traits::DatagramCodec;
const PASSWORD: &[u8] = b"fixed-reference-vector";
fn target() -> Address {
    Address::Ipv6([1; 16])
}
fn reference(name: &str, iv: &[u8]) -> Cipher {
    let kind: ReferenceKind = name.parse().unwrap();
    let mut key = vec![0; kind.key_len()];
    if name == "table" {
        key = PASSWORD.to_vec();
    } else {
        shadowsocks_crypto::v1::openssl_bytes_to_key(PASSWORD, &mut key);
    }
    Cipher::new(kind, &key, iv)
}
#[test]
fn complete_v1_catalog_matches_pinned_reference_datagrams() {
    for &name in shadowsocks_crypto::available_ciphers()
        .iter()
        .filter(|name| !name.starts_with("2022-"))
    {
        let kind = CipherKind::from_str(name).unwrap_or_else(|| panic!("missing {name}"));
        let codec = ShadowsocksDatagramCodec::new(kind, PASSWORD);
        for payload in [vec![], vec![42; 4096]] {
            let wire = codec.encode(&target(), 53, &payload).unwrap();
            let (iv, encrypted) = wire.split_at(kind.salt_len());
            let mut expected = shadowsocks::build_target_data(&target(), 53, &payload).unwrap();
            expected.resize(expected.len() + kind.tag_len(), 0);
            reference(name, iv).encrypt_packet(&mut expected);
            assert_eq!(encrypted, expected, "{name}");
            assert_eq!(codec.decode(&wire), Some((target(), 53, payload)), "{name}");
        }
    }
}
#[tokio::test]
async fn complete_v1_catalog_continues_tcp_state_in_both_directions() {
    for &name in shadowsocks_crypto::available_ciphers()
        .iter()
        .filter(|name| !name.starts_with("2022-"))
    {
        let kind = CipherKind::from_str(name).unwrap();
        let config =
            ShadowsocksTcpConnectConfig::from_config(name, core::str::from_utf8(PASSWORD).unwrap())
                .unwrap();
        let session = Session::new(
            0,
            target(),
            443,
            Network::Tcp,
            ProtocolType::new("shadowsocks"),
        );
        let mut socket = Socket::new(vec![]);
        socket.fragmented_io = true;
        let outbound = config
            .establish_tcp_session(&mut socket, &session)
            .await
            .unwrap();
        let request = socket.output.clone();
        let mut writer = config.wrap_outbound_stream(socket, outbound);
        writer.write_all(b"first").await.unwrap();
        let mut flush = Box::pin(writer.flush());
        std::future::poll_fn(|cx| {
            use std::future::Future;
            assert!(flush.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        drop(flush);
        writer.flush().await.unwrap();
        writer.write_all(b"second").await.unwrap();
        writer.shutdown().await.unwrap();
        let request = request.lock().unwrap().clone();
        let mut accepted_socket = Socket::new(request.clone());
        accepted_socket.fragment = true;
        accepted_socket.fragmented_io = true;
        let replies = accepted_socket.output.clone();
        let profile = ShadowsocksInboundProfile::from_config_cipher_password(
            name,
            core::str::from_utf8(PASSWORD).unwrap(),
        )
        .unwrap();
        let (accepted, mut inbound) = ShadowsocksInboundTcpAcceptor::new(profile)
            .accept_stream(accepted_socket)
            .await
            .unwrap_or_else(|error| panic!("{name}: {error}"));
        assert_eq!(accepted.target, target());
        let mut payload = vec![];
        inbound.read_to_end(&mut payload).await.unwrap();
        assert_eq!(payload, b"firstsecond", "{name}");
        inbound.write_all(b"reply-one").await.unwrap();
        inbound.flush().await.unwrap();
        inbound.write_all(b"reply-two").await.unwrap();
        inbound.shutdown().await.unwrap();
        let reply = replies.lock().unwrap().clone();
        let mut sink = Socket::new(vec![]);
        let session = config
            .establish_tcp_session(&mut sink, &session)
            .await
            .unwrap();
        let mut socket = Socket::new(reply);
        socket.fragmented_io = true;
        let mut response = config.wrap_outbound_stream(socket, session);
        if !kind.is_stream() {
            let mut byte = [0];
            let mut read = Box::pin(response.read(&mut byte));
            std::future::poll_fn(|cx| {
                use std::future::Future;
                assert!(read.as_mut().poll(cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            drop(read);
        }
        let mut payload = vec![];
        response.read_to_end(&mut payload).await.unwrap();
        assert_eq!(payload, b"reply-onereply-two", "{name}");
        if kind.is_stream() {
            let (iv, encrypted) = request.split_at(kind.salt_len());
            let mut plain = encrypted.to_vec();
            assert!(reference(name, iv).decrypt_packet(&mut plain));
            assert_eq!(
                plain,
                shadowsocks::build_target_data(&target(), 443, b"firstsecond").unwrap(),
                "{name}"
            );
        }
    }
}

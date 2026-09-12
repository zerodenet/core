#![cfg(all(feature = "runtime", feature = "blake3"))]
#[path = "support/socket.rs"]
mod socket;
use shadowsocks::{CipherKind, ShadowsocksOutbound};
use shadowsocks_crypto::v2::{tcp::TcpCipher, udp::UdpCipher};
use zero_core::{Address, Network, ProtocolType, Session};
use zero_traits::DatagramCodec;

#[tokio::test]
async fn chacha8_tcp_and_udp_match_the_pinned_optional_reference_backend() {
    let cipher = CipherKind::Blake3Chacha8Poly1305;
    let reference = shadowsocks_crypto::CipherKind::AEAD2022_BLAKE3_CHACHA8_POLY1305;
    let key = b"0123456789abcdef0123456789abcdef";
    let password = b"MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=";
    let address = Address::Domain("example.com".into());
    let target = Session::new(
        0,
        address.clone(),
        443,
        Network::Tcp,
        ProtocolType::new("shadowsocks"),
    );
    let mut socket = socket::Socket::new(vec![]);
    ShadowsocksOutbound
        .send_request(&mut socket, &target, cipher, password)
        .await
        .unwrap();
    let bytes = socket.output.lock().unwrap().clone();
    let mut decoder = TcpCipher::new(reference, key, &bytes[..32]);
    let mut fixed = bytes[32..59].to_vec();
    assert!(decoder.decrypt_packet(&mut fixed));
    let (_, _, size) = shadowsocks::parse_2022_request_fixed_header(&fixed[..11]).unwrap();
    let mut variable = bytes[59..59 + size as usize + 16].to_vec();
    assert!(decoder.decrypt_packet(&mut variable));
    assert_eq!(
        shadowsocks::parse_2022_request_var_header(&variable[..size as usize])
            .unwrap()
            .0,
        address
    );
    let codec = shadowsocks::udp::ShadowsocksDatagramCodec::new(cipher, password);
    let mut inbound = shadowsocks::udp::ShadowsocksInboundUdpCodec::new(cipher, password);
    for payload in [b"".as_slice(), b"payload".as_slice()] {
        let wire = codec.encode(&address, 443, payload).unwrap();
        let nonce = &wire[..24];
        let mut plain = wire[24..].to_vec();
        let decoder = UdpCipher::new(reference, key, 0);
        assert!(decoder.decrypt_packet(nonce, &mut plain));
        plain.truncate(plain.len() - 16);
        let padding = u16::from_be_bytes([plain[25], plain[26]]) as usize;
        assert!(padding < 900);
        if !payload.is_empty() {
            assert_eq!(padding, 0);
        }
        let (decoded, port, offset) =
            shadowsocks::parse_target_data(&plain[27 + padding..]).unwrap();
        assert_eq!((decoded, port), (address.clone(), 443));
        assert_eq!(&plain[27 + padding + offset..], payload);
        let request = inbound.decode_request(&wire).unwrap();
        let response = inbound
            .encode_response(request.client_session_id(), &address, 443, payload)
            .unwrap();
        let mut reference_response = response[24..].to_vec();
        assert!(decoder.decrypt_packet(&response[..24], &mut reference_response));
        assert_eq!(
            codec.decode(&response),
            Some((address.clone(), 443, payload.to_vec()))
        );
    }
}

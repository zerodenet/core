use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf};
use zero_core::{Address, Network, ProtocolType, Session};
use zero_traits::AsyncSocket;

use vmess::inbound::{VmessInbound, VmessInboundProfile};
use vmess::outbound::{PreparedVmessOutboundRequestBundle, VmessOutbound};
use vmess::udp::VmessInboundUdpSession;
use vmess::VmessCipher;
use zero_traits::UdpPacketFraming;

struct TestSocket(DuplexStream);

impl AsyncSocket for TestSocket {
    type Error = io::Error;

    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        AsyncReadExt::read(&mut self.0, buf).await
    }

    async fn write_all(&mut self, buf: &[u8]) -> Result<(), Self::Error> {
        AsyncWriteExt::write_all(&mut self.0, buf).await?;
        AsyncWriteExt::flush(&mut self.0).await
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        AsyncWriteExt::shutdown(&mut self.0).await
    }
}

impl AsyncRead for TestSocket {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl AsyncWrite for TestSocket {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

#[tokio::test]
async fn tcp_stream_encrypts_body_in_both_directions_for_all_ciphers() {
    for cipher in [
        VmessCipher::Aes128Gcm,
        VmessCipher::Chacha20Poly1305,
        VmessCipher::None,
        VmessCipher::Zero,
    ] {
        roundtrip_cipher(cipher).await;
    }
}

#[tokio::test]
async fn tcp_stream_shutdown_sends_body_termination_for_all_ciphers() {
    for cipher in [
        VmessCipher::Aes128Gcm,
        VmessCipher::Chacha20Poly1305,
        VmessCipher::None,
        VmessCipher::Zero,
    ] {
        shutdown_roundtrip_cipher(cipher).await;
    }
}

#[test]
fn cipher_auto_maps_to_aead_baseline() {
    assert_eq!(VmessCipher::from_name("auto"), Some(VmessCipher::Aes128Gcm));
}

#[test]
fn inbound_profile_allows_an_empty_authorization_set() {
    let profile = VmessInboundProfile::from_config_parts(Vec::<
        vmess::inbound::VmessInboundUserConfigParts,
    >::new())
    .expect("empty managed user set");
    assert_eq!(profile.user_count(), 0);
}

#[test]
fn transport_runtime_atomically_replaces_shared_inbound_users() {
    use vmess::transport::{VmessInboundUserRef, VmessTransportRuntime};

    let runtime = VmessTransportRuntime::default();
    let old_users = [VmessInboundUserRef {
        id: "11111111-1111-1111-1111-111111111111",
        cipher: "aes-128-gcm",
        principal_key: Some("account:old"),
        up_bps: None,
        down_bps: None,
        device_limit: None,
        quota_remaining_bytes: None,
        policy_revision: None,
    }];
    let profile = runtime
        .replace_inbound_profile("vmess-in", &old_users)
        .expect("initial VMess profile");
    assert_eq!(profile.user_count(), 1);

    let new_users = [VmessInboundUserRef {
        id: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        cipher: "chacha20-poly1305",
        principal_key: Some("account:new"),
        up_bps: Some(1_000),
        down_bps: Some(2_000),
        device_limit: Some(2),
        quota_remaining_bytes: Some(4096),
        policy_revision: Some(2),
    }];
    let replacement = runtime
        .replace_inbound_profile("vmess-in", &new_users)
        .expect("replacement VMess profile");
    assert_eq!(profile.user_count(), 1);
    assert_eq!(replacement.user_count(), 1);

    runtime
        .replace_inbound_profile("vmess-in", &[])
        .expect("empty VMess profile");
    assert_eq!(profile.user_count(), 0);
}

#[test]
fn udp_packet_framing_roundtrips_domain_target() {
    let target = Address::Domain("example.com".to_owned());
    let payload = b"vmess udp payload";
    let encoded =
        <VmessOutbound as UdpPacketFraming<vmess::udp::VmessUdpPacketTarget>>::encode_udp_packet(
            &VmessOutbound,
            &vmess::udp::VmessUdpPacketTarget {
                address: &target,
                port: 53,
                payload,
            },
        )
        .expect("encode vmess udp packet");

    let decoded =
        <VmessOutbound as UdpPacketFraming<vmess::udp::VmessUdpPacketTarget>>::decode_udp_packet(
            &VmessOutbound,
            &encoded,
        )
        .expect("decode vmess udp packet");

    assert_eq!(decoded.target(), &target);
    assert_eq!(decoded.port(), 53);
    assert_eq!(decoded.payload(), payload);
}

#[tokio::test]
async fn udp_response_encoding_wraps_packet_mode_and_preserves_raw_mode() {
    let target = Address::Domain("example.com".to_owned());
    let default_target = Address::Domain("fallback.example".to_owned());
    let request =
        <VmessOutbound as UdpPacketFraming<vmess::udp::VmessUdpPacketTarget>>::encode_udp_packet(
            &VmessOutbound,
            &vmess::udp::VmessUdpPacketTarget {
                address: &Address::Domain("packet.example".to_owned()),
                port: 5353,
                payload: b"dns",
            },
        )
        .expect("build packet");
    let mut udp_session = VmessInboundUdpSession::new(default_target, 53);
    udp_session
        .decode_inbound_dispatch(&request)
        .expect("enter packet response mode");

    let (mut client, mut server) = tokio::io::duplex(1024);
    udp_session
        .write_response_tokio(&mut client, &target, 5353, b"dns")
        .await
        .expect("encode packet response");
    let mut packet = Vec::new();
    client.shutdown().await.expect("shutdown writer");
    server
        .read_to_end(&mut packet)
        .await
        .expect("read packet response");
    let decoded =
        <VmessOutbound as UdpPacketFraming<vmess::udp::VmessUdpPacketTarget>>::decode_udp_packet(
            &VmessOutbound,
            &packet,
        )
        .expect("decode packet response");
    assert_eq!(decoded.target(), &target);
    assert_eq!(decoded.port(), 5353);
    assert_eq!(decoded.payload(), b"dns");

    let mut raw_session = VmessInboundUdpSession::new(Address::Ipv4([127, 0, 0, 1]), 53);
    raw_session
        .decode_inbound_dispatch(b"raw")
        .expect("enter raw response mode");
    let (mut client, mut server) = tokio::io::duplex(1024);
    raw_session
        .write_response_tokio(&mut client, &Address::Ipv4([127, 0, 0, 1]), 53, b"raw")
        .await
        .expect("encode raw response");
    let mut raw = Vec::new();
    client.shutdown().await.expect("shutdown writer");
    server
        .read_to_end(&mut raw)
        .await
        .expect("read raw response");
    assert_eq!(raw, b"raw");
}

#[test]
fn inbound_udp_payload_decoder_detects_packet_mode_then_requires_packets() {
    let default_target = Address::Domain("fallback.example".to_owned());
    let packet =
        <VmessOutbound as UdpPacketFraming<vmess::udp::VmessUdpPacketTarget>>::encode_udp_packet(
            &VmessOutbound,
            &vmess::udp::VmessUdpPacketTarget {
                address: &Address::Domain("packet.example".to_owned()),
                port: 5353,
                payload: b"dns",
            },
        )
        .expect("build packet");
    let mut udp_session = VmessInboundUdpSession::new(default_target, 53);
    let decoded = udp_session
        .decode_inbound_dispatch(&packet)
        .expect("decode packet payload");
    assert_eq!(
        decoded.target(),
        &Address::Domain("packet.example".to_owned())
    );
    assert_eq!(decoded.port(), 5353);
    assert_eq!(decoded.payload(), b"dns");

    assert!(udp_session.decode_inbound_dispatch(b"raw").is_err());
}

#[test]
fn inbound_udp_payload_decoder_falls_back_to_raw_mode() {
    let default_target = Address::Ipv4([10, 0, 0, 1]);
    let mut udp_session = VmessInboundUdpSession::new(default_target.clone(), 9999);
    let decoded = udp_session
        .decode_inbound_dispatch(b"raw")
        .expect("decode raw payload");
    assert_eq!(decoded.target(), &default_target);
    assert_eq!(decoded.port(), 9999);
    assert_eq!(decoded.payload(), b"raw");
}

#[tokio::test]
async fn mux_udp_response_encoding_wraps_packet_mode_before_mux_frame() {
    let target = Address::Ipv4([8, 8, 8, 8]);
    let request =
        <VmessOutbound as UdpPacketFraming<vmess::udp::VmessUdpPacketTarget>>::encode_udp_packet(
            &VmessOutbound,
            &vmess::udp::VmessUdpPacketTarget {
                address: &target,
                port: 53,
                payload: b"query",
            },
        )
        .expect("build packet");
    let mut udp_session = VmessInboundUdpSession::new(target.clone(), 53);
    udp_session
        .decode_inbound_dispatch(&request)
        .expect("enter packet mux response mode");
    let frame = udp_session
        .encode_mux_response_packet(7, &target, 53, b"query")
        .expect("encode mux udp response");
    let (session_id, payload) = decode_mux_frame_payload(&frame);
    assert_eq!(session_id, 7);
    let packet =
        <VmessOutbound as UdpPacketFraming<vmess::udp::VmessUdpPacketTarget>>::decode_udp_packet(
            &VmessOutbound,
            &payload,
        )
        .expect("decode mux udp payload");
    assert_eq!(packet.target(), &target);
    assert_eq!(packet.port(), 53);
    assert_eq!(packet.payload(), b"query");
}

fn decode_mux_frame_payload(frame: &[u8]) -> (u16, Vec<u8>) {
    assert!(
        frame.len() >= 6,
        "vmess mux frame should contain metadata and length"
    );
    let meta_len = u16::from_be_bytes([frame[0], frame[1]]) as usize;
    assert!(
        frame.len() >= 2 + meta_len + 2,
        "vmess mux frame metadata should fit"
    );
    let meta = &frame[2..2 + meta_len];
    let session_id = u16::from_be_bytes([meta[0], meta[1]]);
    let option = meta[3];
    if option & 0x01 == 0 {
        return (session_id, Vec::new());
    }

    let payload_len_offset = 2 + meta_len;
    let payload_len =
        u16::from_be_bytes([frame[payload_len_offset], frame[payload_len_offset + 1]]) as usize;
    let payload_offset = payload_len_offset + 2;
    assert!(
        frame.len() >= payload_offset + payload_len,
        "vmess mux frame payload should fit"
    );
    (
        session_id,
        frame[payload_offset..payload_offset + payload_len].to_vec(),
    )
}

async fn roundtrip_cipher(cipher: VmessCipher) {
    let (client_io, server_io) = tokio::io::duplex(128 * 1024);
    let target_session = Session::new(
        0,
        Address::Domain("example.com".to_owned()),
        443,
        Network::Tcp,
        ProtocolType::new("vmess"),
    );
    let upload = build_payload(37_000);
    let download = build_payload(21_000);

    let server_upload = upload.clone();
    let server_download = download.clone();
    let server = tokio::spawn(async move {
        let socket = TestSocket(server_io);
        let profile = VmessInboundProfile::from_config_users([(
            "11111111-2222-3333-4444-555555555555".to_owned(),
            cipher.name().to_owned(),
            None::<String>,
            None::<u64>,
            None::<u64>,
            None::<u32>,
            None::<u64>,
            None::<u64>,
        )])
        .expect("server profile");
        let (accepted_session, mut stream) = profile
            .accept_tcp_stream(VmessInbound, socket)
            .await
            .expect("server accept");

        assert_eq!(
            accepted_session.target,
            Address::Domain("example.com".to_owned())
        );
        assert_eq!(accepted_session.port, 443);

        let mut received = vec![0_u8; server_upload.len()];
        stream
            .read_exact(&mut received)
            .await
            .expect("server read upload");
        assert_eq!(received, server_upload);

        stream
            .write_all(&server_download)
            .await
            .expect("server write download");
        stream.flush().await.expect("server flush");
    });

    let socket = TestSocket(client_io);
    let request = PreparedVmessOutboundRequestBundle::from_config(
        "11111111-2222-3333-4444-555555555555",
        cipher.name(),
        None,
    )
    .expect("vmess request");
    let (mut stream, request_bytes) = request
        .establish_tcp_outbound_stream(socket, &target_session)
        .await
        .expect("client handshake");
    assert!(request_bytes > 0);

    stream
        .write_all(&upload)
        .await
        .expect("client write upload");
    stream.flush().await.expect("client flush");

    let mut received = vec![0_u8; download.len()];
    stream
        .read_exact(&mut received)
        .await
        .expect("client read download");
    assert_eq!(received, download);

    server.await.expect("server task");
}

async fn shutdown_roundtrip_cipher(cipher: VmessCipher) {
    let (client_io, server_io) = tokio::io::duplex(128 * 1024);
    let target_session = Session::new(
        0,
        Address::Domain("example.com".to_owned()),
        443,
        Network::Tcp,
        ProtocolType::new("vmess"),
    );
    let upload = build_payload(8192);

    let server_upload = upload.clone();
    let server = tokio::spawn(async move {
        let socket = TestSocket(server_io);
        let profile = VmessInboundProfile::from_config_users([(
            "11111111-2222-3333-4444-555555555555".to_owned(),
            cipher.name().to_owned(),
            None::<String>,
            None::<u64>,
            None::<u64>,
            None::<u32>,
            None::<u64>,
            None::<u64>,
        )])
        .expect("server profile");
        let (_accepted_session, mut stream) = profile
            .accept_tcp_stream(VmessInbound, socket)
            .await
            .expect("server accept");
        let mut received = Vec::new();
        stream
            .read_to_end(&mut received)
            .await
            .expect("server read to eof");
        assert_eq!(received, server_upload);
    });

    let socket = TestSocket(client_io);
    let request = PreparedVmessOutboundRequestBundle::from_config(
        "11111111-2222-3333-4444-555555555555",
        cipher.name(),
        None,
    )
    .expect("vmess request");
    let (mut stream, request_bytes) = request
        .establish_tcp_outbound_stream(socket, &target_session)
        .await
        .expect("client handshake");
    assert!(request_bytes > 0);

    stream
        .write_all(&upload)
        .await
        .expect("client write upload");
    stream.shutdown().await.expect("client shutdown");

    server.await.expect("server task");
}

fn build_payload(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

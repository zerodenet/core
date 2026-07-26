use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use vless::inbound::{VlessInbound, VlessInboundProfile, VlessUser, VlessUserStore};
use vless::outbound::VlessOutbound;
use vless::transport::VlessInboundUserRef;
use vless::udp::{decode_inbound_dispatch, encode_response_packet, VlessUdpPacketV2Codec};
use vless::{format_uuid, parse_uuid};
use zero_core::{Address, Error, Network, ProtocolType, Session};
use zero_traits::AsyncSocket;
use zero_traits::UdpPacketFraming;

const USER_ID: &str = "11111111-2222-3333-4444-555555555555";
const REPLACEMENT_USER_ID: &str = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";

fn tcp_request(user_id: &str) -> Vec<u8> {
    let id = parse_uuid(user_id).expect("uuid");
    let mut request = vec![0x00];
    request.extend_from_slice(&id);
    request.extend_from_slice(&[
        0x00, // addon length
        0x01, // tcp command
        0x01, 0xbb, // port 443
        0x02, // domain
        0x0b,
    ]);
    request.extend_from_slice(b"example.com");
    request
}

#[derive(Debug, Default)]
struct MockSocket {
    reads: VecDeque<u8>,
    writes: Vec<u8>,
    shutdown_called: bool,
}

impl MockSocket {
    fn new(input: &[u8]) -> Self {
        Self {
            reads: input.iter().copied().collect(),
            writes: Vec::new(),
            shutdown_called: false,
        }
    }
}

impl AsyncSocket for MockSocket {
    type Error = ();

    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        let mut read = 0;

        while read < buf.len() {
            let Some(byte) = self.reads.pop_front() else {
                break;
            };
            buf[read] = byte;
            read += 1;
        }

        Ok(read)
    }

    async fn write_all(&mut self, buf: &[u8]) -> Result<(), Self::Error> {
        self.writes.extend_from_slice(buf);
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        self.shutdown_called = true;
        Ok(())
    }
}

impl AsyncRead for MockSocket {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        while buf.remaining() > 0 {
            let Some(byte) = self.reads.pop_front() else {
                break;
            };
            buf.put_slice(&[byte]);
        }
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for MockSocket {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.writes.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.shutdown_called = true;
        Poll::Ready(Ok(()))
    }
}

struct TestUsers {
    id: [u8; 16],
}

impl VlessUserStore for TestUsers {
    fn find_user(&self, id: &[u8; 16]) -> Option<VlessUser> {
        if id == &self.id {
            Some(VlessUser {
                principal_key: Some("user:10001".to_owned()),
                flow: None,
                up_bps: None,
                down_bps: None,
                device_limit: Some(2),
                quota_remaining_bytes: Some(4096),
                policy_revision: Some(2),
            })
        } else {
            None
        }
    }
}

#[test]
fn parses_and_formats_uuid() {
    let id = parse_uuid(USER_ID).expect("uuid");

    assert_eq!(format_uuid(&id), USER_ID);
    assert_eq!(parse_uuid("11111111222233334444555555555555"), Ok(id));
    assert!(parse_uuid("not-a-uuid").is_err());
}

#[tokio::test]
async fn shared_inbound_profile_atomically_replaces_authorized_users() {
    let profile = VlessInboundProfile::from_config_users([VlessInboundUserRef {
        id: USER_ID,
        flow: None,
        principal_key: Some("account:old"),
        up_bps: None,
        down_bps: None,
        device_limit: None,
        quota_remaining_bytes: None,
        policy_revision: None,
    }])
    .expect("initial profile");
    let shared_profile = profile.clone();

    let accepted = shared_profile
        .accept_tcp_with_auth(VlessInbound, &mut MockSocket::new(&tcp_request(USER_ID)))
        .await
        .expect("initial user accepted");
    assert_eq!(
        accepted.auth.and_then(|auth| auth.principal_key),
        Some("account:old".to_owned())
    );

    profile
        .replace_config_users([VlessInboundUserRef {
            id: REPLACEMENT_USER_ID,
            flow: None,
            principal_key: Some("account:new"),
            up_bps: None,
            down_bps: None,
            device_limit: Some(2),
            quota_remaining_bytes: Some(4096),
            policy_revision: Some(2),
        }])
        .expect("replace profile");

    assert!(shared_profile
        .accept_tcp_with_auth(VlessInbound, &mut MockSocket::new(&tcp_request(USER_ID)))
        .await
        .is_err());
    let accepted = shared_profile
        .accept_tcp_with_auth(
            VlessInbound,
            &mut MockSocket::new(&tcp_request(REPLACEMENT_USER_ID)),
        )
        .await
        .expect("replacement user accepted");
    assert_eq!(
        accepted.auth.and_then(|auth| auth.principal_key),
        Some("account:new".to_owned())
    );

    profile
        .replace_config_users(std::iter::empty::<VlessInboundUserRef<'_>>())
        .expect("disable every user");
    assert_eq!(shared_profile.user_count(), 0);
    assert!(shared_profile
        .accept_tcp_with_auth(
            VlessInbound,
            &mut MockSocket::new(&tcp_request(REPLACEMENT_USER_ID)),
        )
        .await
        .is_err());
}

#[tokio::test]
async fn inbound_accepts_authorized_tcp_request_with_domain_target() {
    let id = parse_uuid(USER_ID).expect("uuid");
    let mut request = vec![0x00];
    request.extend_from_slice(&id);
    request.extend_from_slice(&[
        0x00, // addon length
        0x01, // tcp command
        0x01, 0xbb, // port 443
        0x02, // domain
        0x0b,
    ]);
    request.extend_from_slice(b"example.com");

    let mut socket = MockSocket::new(&request);
    let users = TestUsers { id };

    let session = VlessInbound
        .handshake_with_auth(&mut socket, &users)
        .await
        .expect("handshake");

    assert_eq!(session.target, Address::Domain("example.com".into()));
    assert_eq!(session.port, 443);
    assert_eq!(session.network, Network::Tcp);
    assert_eq!(session.protocol, ProtocolType::new("vless"));
    let auth = session.auth.expect("auth");
    assert_eq!(auth.scheme, "vless");
    assert_eq!(auth.principal_key.as_deref(), Some("user:10001"));
    assert_eq!(socket.writes, vec![0x00, 0x00]);
}

#[tokio::test]
async fn inbound_rejects_unknown_user() {
    let mut request = vec![0x00];
    request.extend_from_slice(&[9_u8; 16]);
    request.extend_from_slice(&[
        0x00, // addon length
        0x01, // tcp command
        0x00, 0x50, // port 80
        0x01, // ipv4
        127, 0, 0, 1,
    ]);
    let users = TestUsers {
        id: parse_uuid(USER_ID).expect("uuid"),
    };
    let mut socket = MockSocket::new(&request);

    let error = VlessInbound
        .accept_tcp_with_auth(&mut socket, &users)
        .await
        .expect_err("should fail");

    assert_eq!(error, Error::Unsupported("VLESS user is not authorized"));
    assert!(socket.writes.is_empty());
}

#[tokio::test]
async fn outbound_establishes_tcp_tunnel_for_ipv4_target() {
    let id = parse_uuid(USER_ID).expect("uuid");
    let mut socket = MockSocket::new(&[
        0x00, 0x00, // response version + addon length
    ]);
    let session = Session::new(
        0,
        Address::Ipv4([127, 0, 0, 1]),
        8080,
        Network::Tcp,
        ProtocolType::new("vless"),
    );

    vless::outbound::PreparedVlessOutboundRequestBundle::from_config(USER_ID, None, None, None)
        .expect("request bundle")
        .establish_tcp_outbound_tunnel(&mut socket, &session, false)
        .await
        .expect("tunnel");

    let mut expected = vec![0x00];
    expected.extend_from_slice(&id);
    expected.extend_from_slice(&[
        0x00, // addon length
        0x01, // tcp command
        0x1f, 0x90, // port 8080
        0x01, // ipv4
        127, 0, 0, 1,
    ]);
    assert_eq!(socket.writes, expected);
}

#[cfg(feature = "reality")]
#[tokio::test]
async fn outbound_deferred_tcp_tunnel_request_does_not_read_response() {
    let mut socket = MockSocket::new(&[]);
    let request =
        vless::outbound::PreparedVlessOutboundRequestBundle::from_config(USER_ID, None, None, None)
            .expect("request bundle");
    let session = Session::new(
        0,
        Address::Ipv4([127, 0, 0, 1]),
        8080,
        Network::Tcp,
        ProtocolType::new("vless"),
    );

    request
        .establish_tcp_outbound_tunnel(&mut socket, &session, true)
        .await
        .expect("deferred tunnel request");

    let mut expected = vec![0x00];
    expected.extend_from_slice(&parse_uuid(USER_ID).expect("uuid"));
    expected.extend_from_slice(&[
        0x00, // addon length
        0x01, // tcp command
        0x1f, 0x90, // port 8080
        0x01, // ipv4
        127, 0, 0, 1,
    ]);
    assert_eq!(socket.writes, expected);
}

#[tokio::test]
async fn outbound_stream_reports_handshake_traffic() {
    let socket = MockSocket::new(&[
        0x00, 0x00, // response version + addon length
    ]);
    let request =
        vless::outbound::PreparedVlessOutboundRequestBundle::from_config(USER_ID, None, None, None)
            .expect("request bundle");
    let session = Session::new(
        0,
        Address::Ipv4([127, 0, 0, 1]),
        8080,
        Network::Tcp,
        ProtocolType::new("vless"),
    );

    let (_stream, written_bytes, read_bytes) = request
        .establish_tcp_outbound_stream(socket, &session, false)
        .await
        .expect("outbound stream");

    assert_eq!(written_bytes, 26);
    assert_eq!(read_bytes, 2);
}

#[tokio::test]
async fn outbound_establishes_udp_packet_tunnel_and_consumes_response() {
    let id = parse_uuid(USER_ID).expect("uuid");
    let mut socket = MockSocket::new(&[
        0x00, 0x00, // response version + addon length
    ]);
    let session = Session::new(
        0,
        Address::Ipv4([127, 0, 0, 1]),
        5353,
        Network::Udp,
        ProtocolType::new("vless"),
    );

    vless::udp::establish_udp_packet_tunnel(&mut socket, &session, &id)
        .await
        .expect("udp packet tunnel");

    let mut expected = vec![0x00];
    expected.extend_from_slice(&id);
    expected.extend_from_slice(&[
        0x00, // addon length
        0x02, // udp command
        0x14, 0xe9, // port 5353
        0x01, // ipv4
        127, 0, 0, 1,
    ]);
    assert_eq!(socket.writes, expected);
    assert!(socket.reads.is_empty());
}

#[tokio::test]
async fn inbound_accepts_authorized_udp_request_with_ipv4_target() {
    let id = parse_uuid(USER_ID).expect("uuid");
    let mut request = vec![0x00];
    request.extend_from_slice(&id);
    request.extend_from_slice(&[
        0x00, // addon length
        0x02, // udp command
        0x00, 0x35, // port 53
        0x01, // ipv4
        8, 8, 8, 8,
    ]);

    let mut socket = MockSocket::new(&request);
    let users = TestUsers { id };

    let session = VlessInbound
        .handshake_with_auth(&mut socket, &users)
        .await
        .expect("handshake");

    assert_eq!(session.target, Address::Ipv4([8, 8, 8, 8]));
    assert_eq!(session.port, 53);
    assert_eq!(session.network, Network::Udp);
    assert_eq!(session.protocol, ProtocolType::new("vless"));
}

#[test]
fn parse_udp_packet_with_ipv4() {
    let packet =
        <VlessOutbound as UdpPacketFraming<vless::udp::VlessUdpPacketTarget>>::encode_udp_packet(
            &VlessOutbound,
            &vless::udp::VlessUdpPacketTarget {
                address: &Address::Ipv4([8, 8, 8, 8]),
                port: 53,
                payload: b"dns query",
            },
        )
        .expect("build");
    let parsed =
        <VlessOutbound as UdpPacketFraming<vless::udp::VlessUdpPacketTarget>>::decode_udp_packet(
            &VlessOutbound,
            &packet,
        )
        .expect("parse");
    assert_eq!(parsed.target(), &Address::Ipv4([8, 8, 8, 8]));
    assert_eq!(parsed.port(), 53);
    assert_eq!(parsed.payload(), b"dns query");
}

#[test]
fn parse_udp_packet_with_domain() {
    let packet =
        <VlessOutbound as UdpPacketFraming<vless::udp::VlessUdpPacketTarget>>::encode_udp_packet(
            &VlessOutbound,
            &vless::udp::VlessUdpPacketTarget {
                address: &Address::Domain("example.com".into()),
                port: 443,
                payload: b"udp payload",
            },
        )
        .expect("build");
    let parsed =
        <VlessOutbound as UdpPacketFraming<vless::udp::VlessUdpPacketTarget>>::decode_udp_packet(
            &VlessOutbound,
            &packet,
        )
        .expect("parse");
    assert_eq!(parsed.target(), &Address::Domain("example.com".into()));
    assert_eq!(parsed.port(), 443);
    assert_eq!(parsed.payload(), b"udp payload");
}

#[test]
fn build_udp_packet_with_ipv6() {
    let payload = b"hello v6";
    let address = Address::Ipv6([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    let packet =
        <VlessOutbound as UdpPacketFraming<vless::udp::VlessUdpPacketTarget>>::encode_udp_packet(
            &VlessOutbound,
            &vless::udp::VlessUdpPacketTarget {
                address: &address,
                port: 53,
                payload,
            },
        )
        .expect("build");

    let parsed =
        <VlessOutbound as UdpPacketFraming<vless::udp::VlessUdpPacketTarget>>::decode_udp_packet(
            &VlessOutbound,
            &packet,
        )
        .expect("parse");
    assert_eq!(
        parsed.target(),
        &Address::Ipv6([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1])
    );
    assert_eq!(parsed.port(), 53);
    assert_eq!(parsed.payload(), payload);
}

// ── v2 auto-detect tests ──

#[test]
fn parse_udp_v2_with_address() {
    // [marker:2][flags:1(0x01)][port:2][atyp ipv4:1][addr:4][payload]
    let mut packet = vec![
        0x00, 0x00, // v2 marker
        0x01, // flags: has address
        0x00, 0x35, // port 53
        0x01, // ipv4
        8, 8, 8, 8,
    ];
    packet.extend_from_slice(b"dns query v2");

    let parsed = VlessUdpPacketV2Codec
        .decode_packet(&packet, None, None)
        .expect("parse v2");
    assert_eq!(parsed.target(), &Address::Ipv4([8, 8, 8, 8]));
    assert_eq!(parsed.port(), 53);
    assert_eq!(parsed.payload(), b"dns query v2");
}

#[test]
fn parse_udp_v2_without_address_reuse_cache() {
    // [marker:2][flags:1(0x00)][payload]
    let mut packet = vec![
        0x00, 0x00, // v2 marker
        0x00, // flags: no address
    ];
    packet.extend_from_slice(b"reuse address");

    let cached = Address::Domain("example.com".into());
    let parsed = VlessUdpPacketV2Codec
        .decode_packet(&packet, Some(&cached), Some(443))
        .expect("parse v2 reuse");
    assert_eq!(parsed.target(), &cached);
    assert_eq!(parsed.port(), 443);
    assert_eq!(parsed.payload(), b"reuse address");
}

#[test]
fn parse_udp_v2_without_address_fails_without_cache() {
    let packet = vec![0x00, 0x00, 0x00, b'x'];
    let err = VlessUdpPacketV2Codec
        .decode_packet(&packet, None, None)
        .unwrap_err();
    assert!(
        err.to_string().contains("cached"),
        "expected cache error, got: {err}"
    );
}

#[test]
fn parse_udp_v2_falls_back_to_v1() {
    // v1 format: [port:2][atyp:1][addr:4][payload]
    let mut packet = vec![0x00, 0x35, 0x01, 8, 8, 8, 8];
    packet.extend_from_slice(b"v1 fallback");

    let parsed = VlessUdpPacketV2Codec
        .decode_packet(&packet, None, None)
        .expect("v1 fallback");
    assert_eq!(parsed.target(), &Address::Ipv4([8, 8, 8, 8]));
    assert_eq!(parsed.port(), 53);
    assert_eq!(parsed.payload(), b"v1 fallback");
}

#[test]
fn build_udp_v2_with_address() {
    let packet = VlessUdpPacketV2Codec
        .encode_packet(
            &Address::Ipv4([1, 1, 1, 1]),
            8080,
            b"hello",
            false, // include address
        )
        .expect("build v2");
    assert_eq!(&packet[..2], &[0x00, 0x00]); // marker
    assert_eq!(packet[2], 0x01); // flags: has address
    assert_eq!(u16::from_be_bytes([packet[3], packet[4]]), 8080);
    assert_eq!(packet[5], 0x01); // ipv4

    let parsed = VlessUdpPacketV2Codec
        .decode_packet(&packet, None, None)
        .expect("roundtrip");
    assert_eq!(parsed.target(), &Address::Ipv4([1, 1, 1, 1]));
    assert_eq!(parsed.port(), 8080);
    assert_eq!(parsed.payload(), b"hello");
}

#[test]
fn build_udp_v2_omit_address() {
    let packet = VlessUdpPacketV2Codec
        .encode_packet(
            &Address::Ipv4([0, 0, 0, 0]), // unused when omitting
            0,                            // unused when omitting
            b"streaming",
            true, // omit address
        )
        .expect("build v2 omit");
    assert_eq!(&packet[..2], &[0x00, 0x00]); // marker
    assert_eq!(packet[2], 0x00); // flags: no address
    assert_eq!(&packet[3..], b"streaming"); // payload starts after flags
}

#[test]
fn inbound_udp_decoder_parses_client_packet() {
    let packet =
        <VlessOutbound as UdpPacketFraming<vless::udp::VlessUdpPacketTarget>>::encode_udp_packet(
            &VlessOutbound,
            &vless::udp::VlessUdpPacketTarget {
                address: &Address::Domain("dns.example".into()),
                port: 5353,
                payload: b"query",
            },
        )
        .expect("build packet");

    let parsed = decode_inbound_dispatch(&packet).expect("decode inbound packet");

    assert_eq!(parsed.target(), &Address::Domain("dns.example".into()));
    assert_eq!(parsed.port(), 5353);
    assert_eq!(parsed.payload(), b"query");
}

#[test]
fn udp_response_encoder_builds_response_packet() {
    let packet = encode_response_packet(&Address::Ipv4([1, 1, 1, 1]), 53, b"answer")
        .expect("encode response");

    let parsed =
        <VlessOutbound as UdpPacketFraming<vless::udp::VlessUdpPacketTarget>>::decode_udp_packet(
            &VlessOutbound,
            &packet,
        )
        .expect("parse response packet");
    assert_eq!(parsed.target(), &Address::Ipv4([1, 1, 1, 1]));
    assert_eq!(parsed.port(), 53);
    assert_eq!(parsed.payload(), b"answer");
}

#[test]
#[cfg(feature = "reality")]
fn mux_udp_response_encoder_wraps_vless_packet() {
    let frame = vless::udp::encode_mux_response_packet(7, &Address::Ipv4([8, 8, 8, 8]), 53, b"dns")
        .expect("encode mux response");

    assert_eq!(u16::from_be_bytes([frame[0], frame[1]]), 12);
    assert_eq!(u16::from_be_bytes([frame[2], frame[3]]), 7);
    assert_eq!(frame[4], vless::mux::STATUS_KEEP);
    assert_eq!(frame[5], vless::mux::OPTION_DATA);
    assert_eq!(frame[6], vless::mux::NETWORK_UDP);
    assert_eq!(u16::from_be_bytes([frame[7], frame[8]]), 53);
    assert_eq!(frame[9], 0x01);
    assert_eq!(&frame[10..14], &[8, 8, 8, 8]);
    assert_eq!(u16::from_be_bytes([frame[14], frame[15]]), 3);
    assert_eq!(&frame[16..], b"dns");
}

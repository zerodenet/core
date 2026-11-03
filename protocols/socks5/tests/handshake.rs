use std::cell::RefCell;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use socks5::udp::{
    Socks5EstablishedUdpAssociation, Socks5InboundUdpAssociationSession, Socks5InboundUdpCodec,
    Socks5UdpRelayError,
};
use socks5::{
    Socks5Inbound, Socks5Outbound, Socks5OutboundAuth, Socks5PasswordAuth, Socks5Request,
};
use zero_core::{Address, Error, Network, ProtocolType, Session};
use zero_traits::{AsyncSocket, DatagramSocket, IpAddress};

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

struct TestPasswordAuth;

impl Socks5PasswordAuth for TestPasswordAuth {
    fn required(&self) -> bool {
        true
    }

    fn verify(&self, username: &str, password: &str) -> bool {
        username == "alice" && password == "secret"
    }
}

#[derive(Debug, Default, Clone)]
struct MockDatagramSocket {
    state: Arc<Mutex<MockDatagramState>>,
}

#[derive(Debug, Default)]
struct MockDatagramState {
    recv: VecDeque<(Vec<u8>, IpAddress, u16)>,
    sends: Vec<(Vec<u8>, IpAddress, u16)>,
}

impl MockDatagramSocket {
    fn with_recv(packets: Vec<(Vec<u8>, IpAddress, u16)>) -> Self {
        Self {
            state: Arc::new(Mutex::new(MockDatagramState {
                recv: packets.into(),
                sends: Vec::new(),
            })),
        }
    }

    fn sends(&self) -> Vec<(Vec<u8>, IpAddress, u16)> {
        self.state.lock().expect("lock").sends.clone()
    }
}

impl DatagramSocket for MockDatagramSocket {
    type Error = ();

    async fn recv_from(&self, buf: &mut [u8]) -> Result<(usize, IpAddress, u16), Self::Error> {
        let (packet, address, port) = self
            .state
            .lock()
            .expect("lock")
            .recv
            .pop_front()
            .expect("recv packet");
        let read = packet.len();
        buf[..read].copy_from_slice(&packet);
        Ok((read, address, port))
    }

    async fn send_to(&self, buf: &[u8], addr: IpAddress, port: u16) -> Result<(), Self::Error> {
        self.state
            .lock()
            .expect("lock")
            .sends
            .push((buf.to_vec(), addr, port));
        Ok(())
    }
}

fn test_established_udp_association(
    socket: MockDatagramSocket,
) -> Socks5EstablishedUdpAssociation<MockSocket, MockDatagramSocket> {
    Socks5EstablishedUdpAssociation::from_relay_socket_address(
        MockSocket::new(&[]),
        socket,
        zero_traits::SocketAddress {
            ip: IpAddress::V4([127, 0, 0, 1]),
            port: 1080,
        },
    )
}

#[derive(Debug, Default)]
struct CaptureRelayPacketDispatch {
    client_packets: Vec<Vec<u8>>,
    peer_response: Option<(zero_traits::SocketAddress, Vec<u8>)>,
    unexpected_senders: Vec<zero_traits::SocketAddress>,
}

#[tokio::test]
async fn parses_connect_request_with_domain_target() {
    let mut socket = MockSocket::new(&[
        0x05, 0x01, 0x00, // method negotiation
        0x05, 0x01, 0x00, 0x03, // connect + domain
        0x0b, b'e', b'x', b'a', b'm', b'p', b'l', b'e', b'.', b'c', b'o', b'm', 0x01,
        0xbb, // port 443
    ]);

    let session = Socks5Inbound
        .handshake(&mut socket)
        .await
        .expect("handshake");

    assert_eq!(session.target, Address::Domain("example.com".into()));
    assert_eq!(session.port, 443);
    assert_eq!(session.network, Network::Tcp);
    assert_eq!(session.protocol, ProtocolType::new("socks5"));
    assert_eq!(
        socket.writes,
        vec![
            0x05, 0x00, // auth accepted
            0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0 // connect success
        ]
    );
}

#[tokio::test]
async fn parses_connect_request_with_ipv4_target() {
    let mut socket = MockSocket::new(&[
        0x05, 0x01, 0x00, // method negotiation
        0x05, 0x01, 0x00, 0x01, // connect + ipv4
        1, 2, 3, 4, 0x00, 0x50, // port 80
    ]);

    let session = Socks5Inbound
        .handshake(&mut socket)
        .await
        .expect("handshake");

    assert_eq!(session.target, Address::Ipv4([1, 2, 3, 4]));
    assert_eq!(session.port, 80);
}

#[tokio::test]
async fn rejects_unsupported_auth_method() {
    let mut socket = MockSocket::new(&[0x05, 0x01, 0x02]);

    let error = Socks5Inbound
        .handshake(&mut socket)
        .await
        .expect_err("should fail");

    assert_eq!(
        error,
        Error::Unsupported("SOCKS5 auth method is not supported")
    );
    assert_eq!(socket.writes, vec![0x05, 0xff]);
}

#[tokio::test]
async fn accepts_username_password_auth_when_configured() {
    let mut socket = MockSocket::new(&[
        0x05, 0x02, 0x00, 0x02, // methods: no-auth + username/password
        0x01, 0x05, b'a', b'l', b'i', b'c', b'e', 0x06, b's', b'e', b'c', b'r', b'e',
        b't', // username/password auth
        0x05, 0x01, 0x00, 0x01, // connect + ipv4
        1, 2, 3, 4, 0x00, 0x50, // port 80
    ]);

    let session = Socks5Inbound
        .handshake_with_auth(&mut socket, &TestPasswordAuth)
        .await
        .expect("handshake");

    assert_eq!(session.target, Address::Ipv4([1, 2, 3, 4]));
    assert_eq!(session.port, 80);
    assert_eq!(
        socket.writes,
        vec![
            0x05, 0x02, // username/password selected
            0x01, 0x00, // username/password accepted
            0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0 // connect success
        ]
    );
}

#[tokio::test]
async fn rejects_no_auth_when_password_is_required() {
    let mut socket = MockSocket::new(&[0x05, 0x01, 0x00]);

    let error = Socks5Inbound
        .handshake_with_auth(&mut socket, &TestPasswordAuth)
        .await
        .expect_err("should fail");

    assert_eq!(
        error,
        Error::Unsupported("SOCKS5 auth method is not supported")
    );
    assert_eq!(socket.writes, vec![0x05, 0xff]);
}

#[tokio::test]
async fn rejects_invalid_username_password_credentials() {
    let mut socket = MockSocket::new(&[
        0x05, 0x01, 0x02, // method negotiation
        0x01, 0x05, b'a', b'l', b'i', b'c', b'e', 0x05, b'w', b'r', b'o', b'n', b'g',
    ]);

    let error = Socks5Inbound
        .handshake_with_auth(&mut socket, &TestPasswordAuth)
        .await
        .expect_err("should fail");

    assert_eq!(
        error,
        Error::Unsupported("SOCKS5 username/password authentication failed")
    );
    assert_eq!(
        socket.writes,
        vec![
            0x05, 0x02, // username/password selected
            0x01, 0x01 // username/password rejected
        ]
    );
}

#[tokio::test]
async fn rejects_unsupported_command() {
    let mut socket = MockSocket::new(&[
        0x05, 0x01, 0x00, // method negotiation
        0x05, 0x03, 0x00, 0x01, // udp associate + ipv4
        127, 0, 0, 1, 0x00, 0x35,
    ]);

    let error = Socks5Inbound
        .handshake(&mut socket)
        .await
        .expect_err("should fail");

    assert_eq!(error, Error::Unsupported("SOCKS5 command is not supported"));
    assert_eq!(
        socket.writes,
        vec![
            0x05, 0x00, // auth accepted
            0x05, 0x07, 0x00, 0x01, 0, 0, 0, 0, 0, 0
        ]
    );
}

#[tokio::test]
async fn parses_udp_associate_request() {
    let mut socket = MockSocket::new(&[
        0x05, 0x01, 0x00, // method negotiation
        0x05, 0x03, 0x00, 0x01, // udp associate + ipv4
        0, 0, 0, 0, 0x00, 0x00,
    ]);

    let request = Socks5Inbound
        .accept_command(&mut socket)
        .await
        .expect("request");

    match request {
        Socks5Request::UdpAssociate(request) => {
            assert_eq!(request.client_hint, Address::Ipv4([0, 0, 0, 0]));
            assert_eq!(request.client_port, 0);
        }
        Socks5Request::Connect(_) => panic!("expected udp associate"),
    }

    assert_eq!(socket.writes, vec![0x05, 0x00]);
}

#[tokio::test]
async fn outbound_establishes_tunnel_for_domain_target() {
    let mut socket = MockSocket::new(&[
        0x05, 0x00, // auth accepted
        0x05, 0x00, 0x00, 0x01, // connect success + ipv4
        127, 0, 0, 1, 0x00, 0x50,
    ]);
    let session = Session::new(
        0,
        Address::Domain("example.com".into()),
        443,
        Network::Tcp,
        ProtocolType::new("socks5"),
    );

    Socks5Outbound
        .establish_tunnel(&mut socket, &session)
        .await
        .expect("tunnel");

    assert_eq!(
        socket.writes,
        vec![
            0x05, 0x01, 0x00, // auth negotiation
            0x05, 0x01, 0x00, 0x03, 0x0b, b'e', b'x', b'a', b'm', b'p', b'l', b'e', b'.', b'c',
            b'o', b'm', 0x01, 0xbb
        ]
    );
}

#[tokio::test]
async fn outbound_establishes_tunnel_with_username_password_auth() {
    let mut socket = MockSocket::new(&[
        0x05, 0x02, // username/password selected
        0x01, 0x00, // credentials accepted
        0x05, 0x00, 0x00, 0x01, // connect success + ipv4
        127, 0, 0, 1, 0x00, 0x50,
    ]);
    let session = Session::new(
        0,
        Address::Domain("example.com".into()),
        443,
        Network::Tcp,
        ProtocolType::new("socks5"),
    );

    Socks5Outbound
        .establish_tunnel_with_auth(
            &mut socket,
            &session,
            Some(Socks5OutboundAuth {
                username: "upstream",
                password: "secret",
            }),
        )
        .await
        .expect("tunnel");

    assert_eq!(
        socket.writes,
        vec![
            0x05, 0x01, 0x02, // auth negotiation
            0x01, 0x08, b'u', b'p', b's', b't', b'r', b'e', b'a', b'm', 0x06, b's', b'e', b'c',
            b'r', b'e', b't', // username/password credentials
            0x05, 0x01, 0x00, 0x03, 0x0b, b'e', b'x', b'a', b'm', b'p', b'l', b'e', b'.', b'c',
            b'o', b'm', 0x01, 0xbb
        ]
    );
}

#[tokio::test]
async fn outbound_rejects_upstream_failure_reply() {
    let mut socket = MockSocket::new(&[
        0x05, 0x00, // auth accepted
        0x05, 0x04, 0x00, 0x01, // host unreachable
        127, 0, 0, 1, 0x00, 0x50,
    ]);
    let session = Session::new(
        0,
        Address::Ipv4([1, 1, 1, 1]),
        53,
        Network::Tcp,
        ProtocolType::new("socks5"),
    );

    let error = Socks5Outbound
        .establish_tunnel(&mut socket, &session)
        .await
        .expect_err("fail");

    assert_eq!(error, Error::Route("SOCKS5 upstream host unreachable"));
}

#[tokio::test]
async fn outbound_establishes_udp_association() {
    let mut socket = MockSocket::new(&[
        0x05, 0x00, // auth accepted
        0x05, 0x00, 0x00, 0x01, // udp associate success + ipv4
        127, 0, 0, 1, 0x20, 0x00,
    ]);

    let (address, port) = Socks5Outbound
        .establish_udp_association(&mut socket)
        .await
        .expect("udp association");

    assert_eq!(address, Address::Ipv4([127, 0, 0, 1]));
    assert_eq!(port, 0x2000);
    assert_eq!(
        socket.writes,
        vec![
            0x05, 0x01, 0x00, // auth negotiation
            0x05, 0x03, 0x00, 0x01, 0, 0, 0, 0, 0, 0 // udp associate
        ]
    );
}

#[test]
fn builds_and_parses_udp_packet() {
    let codec = Socks5InboundUdpCodec;
    let packet = codec
        .encode_response_to_client(&Address::Domain("example.com".into()), 5353, b"ping")
        .expect("build packet");
    let parsed = codec.decode_request(&packet).expect("parse packet");

    assert_eq!(parsed.target(), &Address::Domain("example.com".into()));
    assert_eq!(parsed.port(), 5353);
    assert_eq!(parsed.payload(), b"ping");
}

#[test]
fn inbound_udp_codec_decodes_requests_and_encodes_responses() {
    let codec = Socks5InboundUdpCodec;
    let request = codec
        .encode_response_to_client(&Address::Domain("example.com".into()), 5353, b"ping")
        .expect("build request");
    let decoded = codec.decode_request(&request).expect("decode request");

    assert_eq!(decoded.target(), &Address::Domain("example.com".into()));
    assert_eq!(decoded.port(), 5353);
    assert_eq!(decoded.payload(), b"ping");

    let response = codec
        .encode_response_to_client(&Address::Ipv4([1, 1, 1, 1]), 53, b"pong")
        .expect("encode response");
    let decoded_response = codec.decode_response(&response).expect("decode response");

    assert_eq!(decoded_response.target(), &Address::Ipv4([1, 1, 1, 1]));
    assert_eq!(decoded_response.port(), 53);
    assert_eq!(decoded_response.payload(), b"pong");
}

#[test]
fn inbound_udp_association_decodes_standard_packet_facts_and_local_dns() {
    use zero_platform_tokio::contract::inbound::InboundDatagramEvent;
    let mut association = Socks5InboundUdpAssociationSession::new();
    let codec = Socks5InboundUdpCodec;
    let sender = zero_traits::SocketAddress {
        ip: IpAddress::V4([127, 0, 0, 1]),
        port: 10000,
    };
    let packet = codec
        .encode_response_to_client(&Address::Domain("example.com".into()), 5353, b"ping")
        .unwrap();
    let event = association
        .decode_datagram_event(sender, &packet)
        .unwrap()
        .unwrap();
    let InboundDatagramEvent::Packet {
        dispatch,
        sender: actual,
        protocol_bytes,
    } = event
    else {
        panic!("expected a business packet");
    };
    assert_eq!(actual, "127.0.0.1:10000".parse().unwrap());
    assert_eq!(protocol_bytes as usize, packet.len() - 4);
    assert_eq!(dispatch.target(), &Address::Domain("example.com".into()));
    assert_eq!(dispatch.port(), 5353);
    assert_eq!(dispatch.payload(), b"ping");
    assert_eq!(dispatch.client_session_id(), None);
    let packet = codec
        .encode_response_to_client(&Address::Domain("dns.example".into()), 53, b"dns")
        .unwrap();
    assert_eq!(
        association.decode_datagram_event(sender, &packet).unwrap(),
        Some(InboundDatagramEvent::Resolve {
            domain: "dns.example".into()
        })
    );
}

#[tokio::test]
async fn udp_relay_wraps_socks5_packet_before_send() {
    let socket = MockDatagramSocket::default();
    let sends = socket.clone();
    let association = test_established_udp_association(socket);

    let sent = association
        .send_packet(&Address::Domain("example.com".into()), 5353, b"ping")
        .await
        .expect("send packet");

    let sends = sends.sends();
    assert_eq!(sends.len(), 1);
    assert_eq!(sends[0].1, IpAddress::V4([127, 0, 0, 1]));
    assert_eq!(sends[0].2, 1080);
    assert_eq!(sent, sends[0].0.len());

    let parsed = Socks5InboundUdpCodec
        .decode_response(&sends[0].0)
        .expect("parse packet");
    assert_eq!(parsed.target(), &Address::Domain("example.com".into()));
    assert_eq!(parsed.port(), 5353);
    assert_eq!(parsed.payload(), b"ping");
}

#[tokio::test]
async fn inbound_udp_association_sends_peer_response_to_current_client() {
    let socket = MockDatagramSocket::default();
    let mut association = Socks5InboundUdpAssociationSession::new();
    let relay_dispatch = RefCell::new(CaptureRelayPacketDispatch::default());

    association.dispatch_relay_packet_with(
        zero_traits::SocketAddress {
            ip: IpAddress::V4([127, 0, 0, 1]),
            port: 10000,
        },
        b"client packet",
        |payload| {
            relay_dispatch
                .borrow_mut()
                .client_packets
                .push(payload.to_vec())
        },
        |sender, payload| {
            relay_dispatch.borrow_mut().peer_response = Some((sender, payload.to_vec()))
        },
        |sender| relay_dispatch.borrow_mut().unexpected_senders.push(sender),
    );
    association.dispatch_relay_packet_with(
        zero_traits::SocketAddress {
            ip: IpAddress::V4([8, 8, 8, 8]),
            port: 53,
        },
        b"pong",
        |payload| {
            relay_dispatch
                .borrow_mut()
                .client_packets
                .push(payload.to_vec())
        },
        |sender, payload| {
            relay_dispatch.borrow_mut().peer_response = Some((sender, payload.to_vec()))
        },
        |sender| relay_dispatch.borrow_mut().unexpected_senders.push(sender),
    );

    assert_eq!(
        relay_dispatch.borrow().client_packets,
        vec![b"client packet".to_vec()]
    );
    let (sender, payload) = relay_dispatch
        .borrow_mut()
        .peer_response
        .take()
        .expect("peer response");

    let written = association
        .send_current_client_peer_response_parts(&socket, sender, &payload)
        .await
        .expect("send response");

    let sends = socket.sends();
    assert_eq!(sends.len(), 1);
    assert_eq!(sends[0].1, IpAddress::V4([127, 0, 0, 1]));
    assert_eq!(sends[0].2, 10000);
    assert_eq!(written, sends[0].0.len());

    let decoded = Socks5InboundUdpCodec
        .decode_response(&sends[0].0)
        .expect("decode response");
    assert_eq!(decoded.target(), &Address::Ipv4([8, 8, 8, 8]));
    assert_eq!(decoded.port(), 53);
    assert_eq!(decoded.payload(), b"pong");
}

#[tokio::test]
async fn inbound_udp_association_dispatches_relay_packet_with_handlers() {
    let mut association = Socks5InboundUdpAssociationSession::new();
    let mut relay_dispatch = CaptureRelayPacketDispatch::default();

    association.dispatch_relay_packet_with(
        zero_traits::SocketAddress {
            ip: IpAddress::V4([127, 0, 0, 1]),
            port: 10000,
        },
        b"client packet",
        |payload| relay_dispatch.client_packets.push(payload.to_vec()),
        |sender, payload| relay_dispatch.peer_response = Some((sender, payload.to_vec())),
        |sender| relay_dispatch.unexpected_senders.push(sender),
    );
    association.dispatch_relay_packet_with(
        zero_traits::SocketAddress {
            ip: IpAddress::V4([8, 8, 8, 8]),
            port: 53,
        },
        b"pong",
        |payload| relay_dispatch.client_packets.push(payload.to_vec()),
        |sender, payload| relay_dispatch.peer_response = Some((sender, payload.to_vec())),
        |sender| relay_dispatch.unexpected_senders.push(sender),
    );

    assert_eq!(
        relay_dispatch.client_packets,
        vec![b"client packet".to_vec()]
    );
    assert_eq!(
        relay_dispatch.peer_response,
        Some((
            zero_traits::SocketAddress {
                ip: IpAddress::V4([8, 8, 8, 8]),
                port: 53,
            },
            b"pong".to_vec()
        ))
    );
    assert!(relay_dispatch.unexpected_senders.is_empty());
}

#[tokio::test]
async fn inbound_udp_association_classifies_relay_packets_in_protocol() {
    let mut association = Socks5InboundUdpAssociationSession::new();

    let client_packet = association.classify_relay_packet(
        zero_traits::SocketAddress {
            ip: IpAddress::V4([127, 0, 0, 1]),
            port: 10000,
        },
        b"client packet",
    );
    match client_packet {
        socks5::udp::Socks5InboundUdpRelayPacketAction::ClientPacket { payload } => {
            assert_eq!(payload, b"client packet");
        }
        other => panic!("expected client packet, got {other:?}"),
    }

    let peer_packet = association.classify_relay_packet(
        zero_traits::SocketAddress {
            ip: IpAddress::V4([8, 8, 8, 8]),
            port: 53,
        },
        b"pong",
    );
    match peer_packet {
        socks5::udp::Socks5InboundUdpRelayPacketAction::PeerResponse { sender, payload } => {
            assert_eq!(
                sender,
                zero_traits::SocketAddress {
                    ip: IpAddress::V4([8, 8, 8, 8]),
                    port: 53,
                }
            );
            assert_eq!(payload, b"pong");
        }
        other => panic!("expected peer response, got {other:?}"),
    }
}

#[tokio::test]
async fn inbound_udp_association_uses_udp_associate_client_hint_when_present() {
    let socket = MockDatagramSocket::default();
    let mut association =
        Socks5Inbound.accept_udp_association(socks5::udp::Socks5UdpAssociateRequest {
            client_hint: Address::Ipv4([127, 0, 0, 1]),
            client_port: 10000,
        });
    let relay_dispatch = RefCell::new(CaptureRelayPacketDispatch::default());

    association.dispatch_relay_packet_with(
        zero_traits::SocketAddress {
            ip: IpAddress::V4([8, 8, 8, 8]),
            port: 53,
        },
        b"pong",
        |payload| {
            relay_dispatch
                .borrow_mut()
                .client_packets
                .push(payload.to_vec())
        },
        |sender, payload| {
            relay_dispatch.borrow_mut().peer_response = Some((sender, payload.to_vec()))
        },
        |sender| relay_dispatch.borrow_mut().unexpected_senders.push(sender),
    );

    let (sender, payload) = relay_dispatch
        .borrow_mut()
        .peer_response
        .take()
        .expect("peer response");

    let written = association
        .send_current_client_peer_response_parts(&socket, sender, &payload)
        .await
        .expect("send response");

    let sends = socket.sends();
    assert_eq!(sends.len(), 1);
    assert_eq!(sends[0].1, IpAddress::V4([127, 0, 0, 1]));
    assert_eq!(sends[0].2, 10000);
    assert_eq!(written, sends[0].0.len());
}

#[tokio::test]
async fn udp_relay_rejects_packets_from_unexpected_sender() {
    let socket = MockDatagramSocket::with_recv(vec![(
        b"payload".to_vec(),
        IpAddress::V4([127, 0, 0, 2]),
        1080,
    )]);
    let association = test_established_udp_association(socket);

    let error = association
        .recv_packet(&mut [0_u8; 32])
        .await
        .expect_err("unexpected sender");

    assert_eq!(
        error,
        Socks5UdpRelayError::Protocol(Error::Protocol(
            "unexpected UDP sender from SOCKS5 upstream"
        ))
    );
}

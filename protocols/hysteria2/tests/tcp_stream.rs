use std::future::{poll_fn, Future};
use std::task::{Context, Poll, Waker};

use hysteria2::inbound::Hysteria2InboundTcpAcceptor;
use hysteria2::shared::build_tcp_connect_header;
use hysteria2::Hysteria2Outbound;
use zero_core::{Address, Error};
use zero_traits::AsyncSocket;

struct InputStream {
    bytes: Vec<u8>,
    position: usize,
    chunk_size: usize,
    split_at: usize,
}

impl InputStream {
    fn new(bytes: Vec<u8>, chunk_size: usize) -> Self {
        Self {
            bytes,
            position: 0,
            chunk_size,
            split_at: usize::MAX,
        }
    }
}

impl AsyncSocket for InputStream {
    type Error = std::io::Error;

    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        // Suspend between reads as a network stream may do.
        let mut pending = true;
        poll_fn(|cx| {
            if pending {
                pending = false;
                cx.waker().wake_by_ref();
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        })
        .await;
        let boundary = if self.position < self.split_at {
            self.split_at.min(self.bytes.len())
        } else {
            self.bytes.len()
        };
        let len = buf.len().min(self.chunk_size).min(boundary - self.position);
        buf[..len].copy_from_slice(&self.bytes[self.position..self.position + len]);
        self.position += len;
        Ok(len)
    }

    async fn write_all(&mut self, _: &[u8]) -> Result<(), Self::Error> {
        panic!("parsing must not write to the stream")
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        panic!("parsing must not close the stream")
    }
}

fn run<F: Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    // This fixture has no external I/O; each read suspends exactly once.
    for _ in 0..100_000 {
        if let Poll::Ready(result) = future.as_mut().poll(&mut context) {
            return result;
        }
    }
    panic!("stream parser failed to complete within the fixture's poll budget")
}

fn assert_request_preserves_payload(header: &[u8], chunk_size: usize, split_at: usize) {
    let payload = b"\x16\x03\x01TLS client hello or HTTP request";
    let mut bytes = header.to_vec();
    bytes.extend_from_slice(payload);
    let mut stream = InputStream::new(bytes, chunk_size);
    stream.split_at = split_at;
    let session = run(Hysteria2InboundTcpAcceptor::new().accept_stream(&mut stream))
        .expect("accept complete TCPRequest");
    assert_eq!(session.target, Address::Domain("example.com".into()));
    assert_eq!(session.port, 443);
    assert_eq!(stream.position, header.len(), "only consume TCPRequest");
    let mut actual = vec![0; payload.len()];
    run(hysteria2::shared::read_exact(&mut stream, &mut actual)).unwrap();
    assert_eq!(actual, payload);
}

#[test]
fn tcp_request_preserves_coalesced_payload() {
    let header = build_tcp_connect_header(&Address::Domain("example.com".into()), 443).unwrap();
    assert_request_preserves_payload(&header, usize::MAX, usize::MAX);
}

#[test]
fn tcp_request_accepts_every_fragment_boundary() {
    let header = build_tcp_connect_header(&Address::Domain("example.com".into()), 443).unwrap();
    assert_request_preserves_payload(&header, 1, usize::MAX);
    for split_at in 1..header.len() {
        assert_request_preserves_payload(&header, usize::MAX, split_at);
    }
}

#[test]
fn tcp_request_consumes_padding_larger_than_old_header_buffer() {
    let mut header = build_tcp_connect_header(&Address::Domain("example.com".into()), 443).unwrap();
    header.pop();
    header.extend_from_slice(&[0x44, 0x00]); // 1024 bytes of padding.
    header.extend_from_slice(&[0xa5; 1024]);
    for chunk_size in [1, 7, usize::MAX] {
        assert_request_preserves_payload(&header, chunk_size, usize::MAX);
    }
}

#[test]
fn tcp_request_rejects_every_truncated_prefix() {
    let mut header = build_tcp_connect_header(&Address::Domain("example.com".into()), 443).unwrap();
    *header.last_mut().unwrap() = 3;
    header.extend_from_slice(b"pad");
    for end in 0..header.len() {
        let mut stream = InputStream::new(header[..end].to_vec(), 1);
        assert!(run(Hysteria2InboundTcpAcceptor::new().accept_stream(&mut stream)).is_err());
    }
}

#[test]
fn tcp_response_preserves_coalesced_payload_and_accepts_fragmentation() {
    // Success, two-byte message length, message, padding length, padding.
    let mut response = vec![0, 0x40, 0x40];
    response.extend_from_slice(&[b'm'; 64]);
    response.extend_from_slice(&[3, b'p', b'a', b'd']);
    let response_len = response.len();
    response.extend_from_slice(b"server greeting");
    for chunk_size in [1, 7, usize::MAX] {
        let mut stream = InputStream::new(response.clone(), chunk_size);
        run(Hysteria2Outbound.read_connect_response(&mut stream)).unwrap();
        assert_eq!(stream.position, response_len);
        assert_eq!(&stream.bytes[stream.position..], b"server greeting");
    }
}

fn varint(value: u64, width: usize) -> Vec<u8> {
    let mut bytes = value.to_be_bytes()[8 - width..].to_vec();
    bytes[0] |= match width {
        1 => 0,
        2 => 0x40,
        4 => 0x80,
        8 => 0xc0,
        _ => panic!("invalid width"),
    };
    bytes
}

#[test]
fn tcp_request_accepts_all_quic_varint_widths() {
    for width in [2, 4, 8] {
        let mut header = varint(0x401, width);
        header.extend(varint(15, width));
        header.extend_from_slice(b"example.com:443");
        header.extend(varint(0, width));
        assert_request_preserves_payload(&header, 1, usize::MAX);
        for split_at in 1..header.len() {
            assert_request_preserves_payload(&header, usize::MAX, split_at);
        }
    }
}

#[test]
fn tcp_request_accepts_reference_length_limits() {
    let authority = format!("{}:443", "a".repeat(2044));
    let mut header = varint(0x401, 2);
    header.extend(varint(authority.len() as u64, 2));
    header.extend_from_slice(authority.as_bytes());
    header.extend(varint(4096, 2));
    header.extend_from_slice(&[0; 4096]);
    let expected = hysteria2::shared::parse_tcp_connect_header(&header).unwrap();
    let mut stream = InputStream::new(header, 7);
    let session = run(Hysteria2InboundTcpAcceptor::new().accept_stream(&mut stream)).unwrap();
    assert_eq!((session.target, session.port), expected);
}

#[test]
fn tcp_request_rejects_oversized_lengths_before_reading_body() {
    for length in [0, 2049, 1 << 32, (1 << 62) - 1] {
        let mut header = varint(0x401, 2);
        header.extend(varint(length, 8));
        let mut stream = InputStream::new(header.clone(), 1);
        assert!(matches!(
            run(Hysteria2InboundTcpAcceptor::new().accept_stream(&mut stream)),
            Err(Error::Protocol("hysteria2: invalid address length"))
        ));
        assert!(hysteria2::shared::parse_tcp_connect_header(&header).is_err());
    }
    for length in [4097, 1 << 32, (1 << 62) - 1] {
        let mut header =
            build_tcp_connect_header(&Address::Domain("example.com".into()), 443).unwrap();
        header.pop();
        header.extend(varint(length, 8));
        let mut stream = InputStream::new(header.clone(), 1);
        assert!(matches!(
            run(Hysteria2InboundTcpAcceptor::new().accept_stream(&mut stream)),
            Err(Error::Protocol("hysteria2: invalid padding length"))
        ));
        assert!(hysteria2::shared::parse_tcp_connect_header(&header).is_err());
    }
}

#[test]
fn tcp_request_rejects_wrong_type_and_malformed_authority() {
    let mut stream = InputStream::new(vec![0], 1);
    assert!(matches!(
        run(Hysteria2InboundTcpAcceptor::new().accept_stream(&mut stream)),
        Err(Error::Protocol("hysteria2: expected TCPRequest"))
    ));
    for authority in [b"\xff:80".as_slice(), b":80", b"host", b"host:65536"] {
        let mut header = varint(0x401, 2);
        header.extend(varint(authority.len() as u64, 1));
        header.extend_from_slice(authority);
        header.push(0);
        let mut stream = InputStream::new(header, 1);
        assert!(run(Hysteria2InboundTcpAcceptor::new().accept_stream(&mut stream)).is_err());
    }
}

#[test]
fn tcp_response_rejects_oversized_lengths_before_reading_body() {
    for length in [2049, 1 << 32, (1 << 62) - 1] {
        let mut response = vec![0];
        response.extend(varint(length, 8));
        let mut stream = InputStream::new(response, 1);
        assert!(matches!(
            run(Hysteria2Outbound.read_connect_response(&mut stream)),
            Err(Error::Protocol("hysteria2: invalid message length"))
        ));
    }
    for length in [4097, 1 << 32, (1 << 62) - 1] {
        let mut response = vec![0, 0];
        response.extend(varint(length, 8));
        let mut stream = InputStream::new(response, 1);
        assert!(matches!(
            run(Hysteria2Outbound.read_connect_response(&mut stream)),
            Err(Error::Protocol("hysteria2: invalid padding length"))
        ));
    }
}

#[test]
fn tcp_response_accepts_reference_limits_and_rejects_truncation_and_error_status() {
    let mut response = vec![0];
    response.extend(varint(2048, 2));
    response.extend_from_slice(&[b'm'; 2048]);
    response.extend(varint(4096, 2));
    response.extend_from_slice(&[0; 4096]);
    run(Hysteria2Outbound.read_connect_response(&mut InputStream::new(response, 7))).unwrap();
    let response = [0, 1, b'm', 1, b'p'];
    for end in 0..response.len() {
        let mut stream = InputStream::new(response[..end].to_vec(), 1);
        assert!(run(Hysteria2Outbound.read_connect_response(&mut stream)).is_err());
    }
    let mut stream = InputStream::new(vec![1, 0, 0], 1);
    assert!(matches!(
        run(Hysteria2Outbound.read_connect_response(&mut stream)),
        Err(Error::Protocol("hysteria2: connect rejected"))
    ));
}

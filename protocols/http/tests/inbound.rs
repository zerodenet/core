use std::collections::VecDeque;

use http::{HttpConnectInbound, HttpConnectResponse, HttpInboundMode};
use zero_core::Address;
use zero_traits::AsyncSocket;

#[derive(Debug, Default)]
struct MockSocket {
    reads: VecDeque<u8>,
    writes: Vec<u8>,
}

impl MockSocket {
    fn new(input: &[u8]) -> Self {
        Self {
            reads: input.iter().copied().collect(),
            writes: Vec::new(),
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
        Ok(())
    }
}

#[tokio::test]
async fn parses_domain_authority() {
    let request = b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\n";
    let mut socket = MockSocket::new(request);

    let request = HttpConnectInbound
        .accept_request(&mut socket)
        .await
        .expect("request");

    assert_eq!(request.mode(), HttpInboundMode::Connect);
    assert_eq!(
        request.session().target,
        Address::Domain("example.com".to_string())
    );
    assert_eq!(request.session().port, 443);
}

#[tokio::test]
async fn parses_ipv4_authority() {
    let request = b"CONNECT 127.0.0.1:8080 HTTP/1.1\r\nHost: 127.0.0.1:8080\r\n\r\n";
    let mut socket = MockSocket::new(request);

    let request = HttpConnectInbound
        .accept_request(&mut socket)
        .await
        .expect("request");

    assert_eq!(request.mode(), HttpInboundMode::Connect);
    assert_eq!(request.session().target, Address::Ipv4([127, 0, 0, 1]));
    assert_eq!(request.session().port, 8080);
}

#[tokio::test]
async fn parses_absolute_form_get_and_rewrites_request_target() {
    let request = b"GET http://192.168.50.1/status?view=full HTTP/1.1\r\nHost: 192.168.50.1\r\nConnection: close\r\n\r\n";
    let mut socket = MockSocket::new(request);

    let request = HttpConnectInbound
        .accept_request(&mut socket)
        .await
        .expect("request");
    let (session, mode, replay) = request.into_parts();

    assert_eq!(mode, HttpInboundMode::Forward);
    assert_eq!(session.target, Address::Ipv4([192, 168, 50, 1]));
    assert_eq!(session.port, 80);
    assert_eq!(
        replay,
        b"GET /status?view=full HTTP/1.1\r\nHost: 192.168.50.1\r\nConnection: close\r\n\r\n"
    );
}

#[tokio::test]
async fn parses_absolute_form_post_with_explicit_port_and_preserves_body() {
    let request = b"POST http://example.com:8080/upload HTTP/1.1\r\nHost: example.com:8080\r\nContent-Length: 4\r\n\r\ndata";
    let mut socket = MockSocket::new(request);

    let request = HttpConnectInbound
        .accept_request(&mut socket)
        .await
        .expect("request");
    let (session, mode, replay) = request.into_parts();

    assert_eq!(mode, HttpInboundMode::Forward);
    assert_eq!(session.target, Address::Domain("example.com".to_string()));
    assert_eq!(session.port, 8080);
    assert_eq!(
        replay,
        b"POST /upload HTTP/1.1\r\nHost: example.com:8080\r\nContent-Length: 4\r\n\r\n"
    );

    let mut body = [0_u8; 4];
    let read = socket.read(&mut body).await.expect("body");
    assert_eq!(read, body.len());
    assert_eq!(&body, b"data");
}

#[tokio::test]
async fn rejects_origin_form_request_without_proxy_target() {
    let request = b"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n";
    let mut socket = MockSocket::new(request);

    let error = HttpConnectInbound
        .accept_request(&mut socket)
        .await
        .expect_err("error");

    assert_eq!(
        error,
        zero_core::Error::Protocol(
            "HTTP forward-proxy request target must use absolute-form"
        )
    );
}

#[tokio::test]
async fn writes_connection_established_response() {
    let mut socket = MockSocket::default();

    HttpConnectInbound
        .send_response(&mut socket, HttpConnectResponse::ConnectionEstablished)
        .await
        .expect("response");

    assert_eq!(
        socket.writes,
        b"HTTP/1.1 200 Connection Established\r\n\r\n"
    );
}

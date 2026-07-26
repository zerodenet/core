//! Round-trip tests for the XHTTP `stream-one` single-connection transport.
//!
//! These exercise both the production H2/H2C `connect_xhttp_stream_one` path
//! and the explicit HTTP/1.1 wire helper against in-process peers. They verify
//! the legacy chunked codec and that the production stream can use one
//! bidirectional carrier as a relay-chain final hop.

#![cfg(feature = "split_http")]

use std::time::Duration;

use tokio::io::{duplex, AsyncReadExt, AsyncWriteExt};
use zero_transport::profile::OwnedSplitHttpProfile;
use zero_transport::split_http::{
    accept_xhttp_stream_one, connect_split_http, connect_xhttp_stream_one,
    connect_xhttp_stream_one_http1,
};

/// Build a config with the given host/path and `auto` mode.
fn cfg(host: &str, path: &str) -> OwnedSplitHttpProfile {
    OwnedSplitHttpProfile {
        host: Some(host.to_string()),
        path: path.to_string(),
        mode: "auto".to_string(),
    }
}

/// Read until `\r\n\r\n` from `s`, returning the raw header bytes (inclusive).
async fn read_headers(s: &mut (impl AsyncReadExt + Unpin)) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 1];
    loop {
        let n = s.read(&mut tmp).await.expect("read header byte");
        assert_ne!(n, 0, "EOF before end of headers");
        buf.push(tmp[0]);
        if buf.len() >= 4 && &buf[buf.len() - 4..] == b"\r\n\r\n" {
            return buf;
        }
    }
}

/// Verify the download decoder reconstructs multi-chunk data correctly.
///
/// The mock server emits two 5-byte chunks (`hello`, `world`) followed by the
/// terminating `0` chunk. The trailing `\r\n` after each chunk's data must be
/// consumed without corrupting the next size line.
#[tokio::test]
async fn stream_one_decodes_multi_chunk_download() {
    let (client, mut server) = duplex(8192);

    let server_task = tokio::spawn(async move {
        // Expect a POST request with chunked upload.
        let req = read_headers(&mut server).await;
        let req = String::from_utf8(req).unwrap();
        assert!(
            req.starts_with("POST /up HTTP/1.1\r\n"),
            "bad request line: {req}"
        );
        assert!(
            req.contains("Transfer-Encoding: chunked"),
            "missing chunked upload header"
        );
        assert!(
            !req.contains("X-Session-Id:"),
            "stream-one must not advertise a paired-session id"
        );
        let referer = req
            .lines()
            .find_map(|line| line.strip_prefix("Referer: "))
            .expect("missing XHTTP padding referer");
        let padding = referer
            .strip_prefix("https://example.com/up?x_padding=")
            .expect("unexpected padding referer");
        assert!(
            (100..=1000).contains(&padding.len()),
            "padding length outside the XHTTP default range: {}",
            padding.len()
        );
        assert!(
            padding.bytes().all(|byte| byte == b'X'),
            "padding must use the default repeat-X method"
        );

        // Respond 200 with a chunked body containing two data chunks.
        server
            .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
            .await
            .unwrap();
        // Deliberately split across writes to test segmentation handling.
        server.write_all(b"5\r\nhel").await.unwrap();
        server.write_all(b"lo\r\n5\r\nworld\r\n").await.unwrap();
        server.write_all(b"0\r\n\r\n").await.unwrap();
        server.flush().await.unwrap();
    });

    let mut stream = connect_xhttp_stream_one_http1(client, &cfg("example.com", "/up"))
        .await
        .expect("connect");

    let mut out = Vec::new();
    stream.read_to_end(&mut out).await.expect("read_to_end");
    assert_eq!(out, b"helloworld", "decoded download must match");

    server_task.await.unwrap();
}

/// A real streaming HTTP server may wait for request-body data before its
/// response becomes observable. Connecting must therefore not wait for `200`,
/// otherwise the carrier and protocol handshake deadlock on each other's first
/// byte.
#[tokio::test]
async fn stream_one_sends_protocol_first_byte_before_waiting_for_response() {
    let (client, mut server) = duplex(8192);

    let server_task = tokio::spawn(async move {
        let _req = read_headers(&mut server).await;
        let mut first_chunk = [0u8; 10];
        server
            .read_exact(&mut first_chunk)
            .await
            .expect("read first request body chunk");
        assert_eq!(&first_chunk, b"5\r\nhello\r\n");

        server
            .write_all(
                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nworld\r\n0\r\n\r\n",
            )
            .await
            .unwrap();
        server.flush().await.unwrap();
    });

    let mut stream = connect_xhttp_stream_one_http1(client, &cfg("example.com", "/up"))
        .await
        .expect("connect must not wait for response headers");
    stream.write_all(b"hello").await.expect("write first chunk");

    let mut response = [0u8; 5];
    stream
        .read_exact(&mut response)
        .await
        .expect("read response after first upload");
    assert_eq!(&response, b"world");

    server_task.await.unwrap();
}

/// Verify the upload encoder frames writes as chunked and the server can read
/// them back, including the terminating `0\r\n\r\n` on shutdown.
#[tokio::test]
async fn stream_one_encodes_multi_chunk_upload() {
    let (client, mut server) = duplex(8192);

    // Server: consume the POST line, reply 200 immediately (empty body for now).
    let server_task = tokio::spawn(async move {
        let _req = read_headers(&mut server).await;
        server
            .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
            .await
            .unwrap();
        server.flush().await.unwrap();

        // Read the client's chunked upload body until the terminating chunk.
        let mut body = Vec::new();
        let mut buf = vec![0u8; 256];
        loop {
            let n = server.read(&mut buf).await.expect("server read");
            if n == 0 {
                break;
            }
            body.extend_from_slice(&buf[..n]);
            if body.windows(5).any(|w| w == b"0\r\n\r\n") {
                break;
            }
        }
        // Expect two encoded chunks: 4\r\ntest\r\n and 4\r\ndata\r\n then 0\r\n\r\n
        let body_str = String::from_utf8_lossy(&body);
        assert!(
            body_str.contains("4\r\ntest\r\n"),
            "first chunk missing: {body_str}"
        );
        assert!(
            body_str.contains("4\r\ndata\r\n"),
            "second chunk missing: {body_str}"
        );
        assert!(body_str.ends_with("0\r\n\r\n"), "terminating chunk missing");
    });

    let mut stream = connect_xhttp_stream_one_http1(client, &cfg("example.com", "/up"))
        .await
        .expect("connect");

    stream.write_all(b"test").await.expect("write test");
    stream.flush().await.expect("flush");
    stream.write_all(b"data").await.expect("write data");
    stream.flush().await.expect("flush");
    stream.shutdown().await.expect("shutdown");

    // Give the server task time to read everything before the test ends.
    tokio::time::timeout(Duration::from_secs(2), server_task)
        .await
        .expect("server task timed out")
        .unwrap();
}

/// Verify that a single download spanning header boundary + first chunk is
/// handled (the response headers and body arrive in one read).
#[tokio::test]
async fn stream_one_handles_pipelined_response_header_and_body() {
    let (client, mut server) = duplex(8192);

    let server_task = tokio::spawn(async move {
        let _req = read_headers(&mut server).await;
        // Headers + body in one write. "ping!" is 5 bytes → size line `5`.
        server
            .write_all(
                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nping!\r\n0\r\n\r\n",
            )
            .await
            .unwrap();
        server.flush().await.unwrap();
    });

    let mut stream = connect_xhttp_stream_one_http1(client, &cfg("example.com", "/up"))
        .await
        .expect("connect");

    let mut out = Vec::new();
    stream.read_to_end(&mut out).await.expect("read_to_end");
    assert_eq!(out, b"ping!");

    server_task.await.unwrap();
}

// ────────────────────────────────────────────────────────────────────
// Two-connection (packet-up / stream-up) decoder — verifies the fix for the
// legacy SplitHttpPairedStream chunked-decoder bug (trailing `\r\n` after each
// chunk + multi-chunk responses). The server replies on the GET socket with a
// multi-chunk chunked body; the client must reconstruct it byte-for-byte.
// ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn two_connection_decodes_multi_chunk_download() {
    // POST socket (upload) and GET socket (download) are independent.
    let (client_post, mut server_post) = duplex(8192);
    let (client_get, mut server_get) = duplex(8192);

    let server_task = tokio::spawn(async move {
        // Consume POST and GET request headers.
        let _post_req = read_headers(&mut server_post).await;
        let _get_req = read_headers(&mut server_get).await;

        // Respond 200 with a chunked body in THREE data chunks.
        server_get
            .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
            .await
            .unwrap();
        server_get.write_all(b"5\r\nhello\r\n").await.unwrap();
        server_get.write_all(b"1\r\n \r\n").await.unwrap();
        server_get.write_all(b"5\r\nworld\r\n").await.unwrap();
        server_get.write_all(b"0\r\n\r\n").await.unwrap();
        server_get.flush().await.unwrap();
    });

    let mut stream = connect_split_http(client_post, client_get, &cfg("example.com", "/up"))
        .await
        .expect("connect");

    let mut out = Vec::new();
    stream.read_to_end(&mut out).await.expect("read_to_end");
    // "hello" + " " + "world" — the trailing \r\n after each chunk must be
    // consumed, not leaked into the decoded output.
    assert_eq!(out, b"hello world", "multi-chunk decode across chunks");

    server_task.await.unwrap();
}

// ────────────────────────────────────────────────────────────────────
// Inbound stream-one round-trip: a client (connect_xhttp_stream_one) talks to
// a server (accept_xhttp_stream_one) over a single duplex pair. Verifies the
// server handshake and that upload/download both flow over one connection.
// ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn inbound_stream_one_round_trip() {
    // One duplex pair = one bidirectional TCP connection.
    let (client_side, server_side) = duplex(8192);

    // Server: accept stream-one, then echo upload → download.
    let server_task = tokio::spawn(async move {
        let mut conn = accept_xhttp_stream_one(server_side, &cfg("example.com", "/up"))
            .await
            .expect("accept");

        // Read the client's upload (chunked-decoded) and echo it back as
        // download (chunked-encoded on the response body).
        let mut buf = [0u8; 64];
        loop {
            let n = conn.read(&mut buf).await.expect("server read");
            if n == 0 {
                break;
            }
            conn.write_all(&buf[..n]).await.expect("server echo");
            conn.flush().await.expect("server flush");
        }
        conn.shutdown().await.expect("server shutdown");
    });

    let mut stream = connect_xhttp_stream_one(client_side, &cfg("example.com", "/up"))
        .await
        .expect("connect");

    // Upload + read back the echo.
    stream.write_all(b"round-trip").await.expect("upload");
    stream.flush().await.expect("flush upload");

    let mut echo = [0u8; 64];
    let n = stream.read(&mut echo).await.expect("read echo");
    assert_eq!(&echo[..n], b"round-trip", "server must echo upload back");

    // Shutdown upload; server sees EOF and shuts down its download.
    stream.shutdown().await.expect("client shutdown");

    tokio::time::timeout(Duration::from_secs(2), server_task)
        .await
        .expect("server task timed out")
        .unwrap();
}

/// Cleartext Xray clients use HTTP/1.1 chunked stream-one. The production
/// inbound must negotiate that wire shape as well as H2/H2C without requiring
/// a separate listener or panel-specific mode.
#[tokio::test]
async fn inbound_stream_one_accepts_http1_round_trip() {
    let (client_side, server_side) = duplex(8192);

    let server_task = tokio::spawn(async move {
        let mut conn = accept_xhttp_stream_one(server_side, &cfg("example.com", "/up"))
            .await
            .expect("accept HTTP/1.1 stream-one");
        let mut upload = [0_u8; 10];
        conn.read_exact(&mut upload)
            .await
            .expect("read HTTP/1.1 upload");
        assert_eq!(&upload, b"http1-wire");
        conn.write_all(b"http1-echo")
            .await
            .expect("write HTTP/1.1 response");
        conn.shutdown().await.expect("shutdown HTTP/1.1 response");
    });

    let mut stream = connect_xhttp_stream_one_http1(client_side, &cfg("example.com", "/up"))
        .await
        .expect("connect HTTP/1.1 stream-one");
    stream
        .write_all(b"http1-wire")
        .await
        .expect("write HTTP/1.1 upload");
    stream.shutdown().await.expect("shutdown HTTP/1.1 upload");
    let mut echoed = [0_u8; 10];
    stream
        .read_exact(&mut echoed)
        .await
        .expect("read HTTP/1.1 echo");
    assert_eq!(&echoed, b"http1-echo");

    tokio::time::timeout(Duration::from_secs(2), server_task)
        .await
        .expect("HTTP/1.1 server task timed out")
        .unwrap();
}

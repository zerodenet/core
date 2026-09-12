#![cfg(feature = "crypto")]

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use shadowsocks::udp::{
    ShadowsocksDatagramCodec, ShadowsocksUdpDecodeContext, ShadowsocksUdpPacketTarget,
};
use shadowsocks::{
    decrypt_tcp_chunk_length, decrypt_tcp_chunk_payload, derive_download_key, derive_key,
    derive_session_key, encrypt_tcp_chunk, parse_target_data, CipherKind, ShadowsocksAccept,
    ShadowsocksAeadStream, ShadowsocksInbound, ShadowsocksInboundProfile, ShadowsocksOutbound,
    ShadowsocksOutboundSession, TCP_CHUNK_SIZE_LEN,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf};
use zero_core::{Address, Network, ProtocolType, Session};
use zero_traits::{AsyncSocket, DatagramCodec, UdpDatagramFraming};

fn supported_ciphers() -> Vec<CipherKind> {
    let mut ciphers = vec![
        CipherKind::Aes128Gcm,
        CipherKind::Aes256Gcm,
        CipherKind::Chacha20Poly1305,
    ];
    #[cfg(feature = "blake3")]
    ciphers.extend([
        CipherKind::Blake3Aes128Gcm,
        CipherKind::Blake3Aes256Gcm,
        CipherKind::Blake3Chacha20Poly1305,
        CipherKind::Blake3Chacha8Poly1305,
    ]);
    ciphers
}

/// Legacy (SIP004) AEAD ciphers only. Used by stream-framing tests that drive
/// the legacy salt + length/payload-chunk response model; 2022 ciphers use a
/// different response header and are covered by dedicated 2022 tests.
fn legacy_ciphers() -> Vec<CipherKind> {
    vec![
        CipherKind::Aes128Gcm,
        CipherKind::Aes256Gcm,
        CipherKind::Chacha20Poly1305,
    ]
}

/// 2022 edition (SIP022) ciphers.
#[cfg(feature = "blake3")]
fn blake3_ciphers() -> Vec<CipherKind> {
    vec![
        CipherKind::Blake3Aes128Gcm,
        CipherKind::Blake3Aes256Gcm,
        CipherKind::Blake3Chacha20Poly1305,
        CipherKind::Blake3Chacha8Poly1305,
    ]
}

fn derive_test_key(cipher: CipherKind, password: &[u8], salt: &[u8]) -> Vec<u8> {
    derive_session_key(cipher, password, salt).expect("derive key")
}

fn password_for_cipher(cipher: CipherKind) -> &'static [u8] {
    match cipher {
        CipherKind::Blake3Aes128Gcm => b"MDEyMzQ1Njc4OWFiY2RlZg==",
        CipherKind::Blake3Aes256Gcm
        | CipherKind::Blake3Chacha20Poly1305
        | CipherKind::Blake3Chacha8Poly1305 => b"MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=",
        _ => b"test-password",
    }
}

#[cfg(feature = "blake3")]
fn cipher_name(cipher: CipherKind) -> &'static str {
    cipher.name()
}

#[cfg(feature = "blake3")]
fn password_str(cipher: CipherKind) -> &'static str {
    match cipher {
        CipherKind::Blake3Aes128Gcm => "MDEyMzQ1Njc4OWFiY2RlZg==",
        CipherKind::Blake3Aes256Gcm
        | CipherKind::Blake3Chacha20Poly1305
        | CipherKind::Blake3Chacha8Poly1305 => "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=",
        _ => "test-password",
    }
}

#[derive(Default)]
struct RecordingSocket {
    writes: Vec<Vec<u8>>,
}

impl AsyncSocket for RecordingSocket {
    type Error = io::Error;

    async fn read(&mut self, _buf: &mut [u8]) -> Result<usize, Self::Error> {
        Ok(0)
    }

    async fn write_all(&mut self, buf: &[u8]) -> Result<(), Self::Error> {
        self.writes.push(buf.to_vec());
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[test]
fn tcp_chunks_roundtrip_all_supported_ciphers() {
    let plaintext = b"hello shadowsocks tcp";

    for cipher in supported_ciphers() {
        let password = password_for_cipher(cipher);
        let salt = vec![0x42_u8; cipher.salt_len()];
        let key = derive_test_key(cipher, password, &salt);
        let mut encrypt_nonce = 0;
        let chunk = encrypt_tcp_chunk(cipher, &key, &mut encrypt_nonce, plaintext)
            .expect("encrypt tcp chunk");
        assert_eq!(encrypt_nonce, 2);

        let mut decrypt_nonce = 0;
        let length_size = TCP_CHUNK_SIZE_LEN + cipher.tag_len();
        let payload_len =
            decrypt_tcp_chunk_length(cipher, &key, &mut decrypt_nonce, &chunk[..length_size])
                .expect("decrypt tcp chunk length");
        let plain = decrypt_tcp_chunk_payload(
            cipher,
            &key,
            &mut decrypt_nonce,
            payload_len,
            &chunk[length_size..],
        )
        .expect("decrypt tcp chunk payload");

        assert_eq!(decrypt_nonce, 2);
        assert_eq!(plain, plaintext, "cipher: {cipher:?}");
    }
}

#[tokio::test]
async fn outbound_writes_salt_and_first_chunk_in_one_write() {
    let cipher = CipherKind::Aes128Gcm;
    let password = b"test-password";
    let session = Session::new(
        0,
        Address::Domain("www.gstatic.com".to_owned()),
        80,
        Network::Tcp,
        ProtocolType::new("shadowsocks"),
    );
    let mut socket = RecordingSocket::default();

    let outbound_session = ShadowsocksOutbound
        .send_request(&mut socket, &session, cipher, password)
        .await
        .expect("send shadowsocks request");

    assert_eq!(socket.writes.len(), 1);
    assert_eq!(outbound_session.next_upload_nonce, 2);

    let request = &socket.writes[0];
    let salt_len = cipher.salt_len();
    let length_size = TCP_CHUNK_SIZE_LEN + cipher.tag_len();
    assert!(request.len() > salt_len + length_size);

    let key = derive_key(password, &request[..salt_len], cipher.key_len()).unwrap();
    let mut nonce = 0;
    let payload_len = decrypt_tcp_chunk_length(
        cipher,
        &key,
        &mut nonce,
        &request[salt_len..salt_len + length_size],
    )
    .unwrap();
    let plain = decrypt_tcp_chunk_payload(
        cipher,
        &key,
        &mut nonce,
        payload_len,
        &request[salt_len + length_size..],
    )
    .unwrap();

    let (target, port, payload_offset) = parse_target_data(&plain).unwrap();
    assert_eq!(target, Address::Domain("www.gstatic.com".to_owned()));
    assert_eq!(port, 80);
    assert_eq!(&plain[payload_offset..], b"");
}

#[tokio::test]
async fn aead_stream_roundtrips_all_supported_ciphers() {
    for cipher in legacy_ciphers() {
        let password = password_for_cipher(cipher).to_vec();
        let upload_salt = vec![0x11_u8; cipher.salt_len()];
        let upload_key = derive_test_key(cipher, &password, &upload_salt);
        let (client_io, mut server_io) = tokio::io::duplex(4096);

        let outbound_session = ShadowsocksOutboundSession {
            legacy: None,
            session_key: upload_key.clone(),
            next_upload_nonce: 0,
            cipher,
            request_salt: Vec::new(),
        };
        let mut stream =
            ShadowsocksAeadStream::outbound(client_io, outbound_session, password.clone());

        stream.write_all(b"ping").await.unwrap();
        stream.flush().await.unwrap();

        let mut encrypted_len = vec![0_u8; TCP_CHUNK_SIZE_LEN + cipher.tag_len()];
        server_io.read_exact(&mut encrypted_len).await.unwrap();
        let mut upload_nonce = 0;
        let payload_len =
            decrypt_tcp_chunk_length(cipher, &upload_key, &mut upload_nonce, &encrypted_len)
                .unwrap();
        let mut encrypted_payload = vec![0_u8; payload_len + cipher.tag_len()];
        server_io.read_exact(&mut encrypted_payload).await.unwrap();
        let plain = decrypt_tcp_chunk_payload(
            cipher,
            &upload_key,
            &mut upload_nonce,
            payload_len,
            &encrypted_payload,
        )
        .unwrap();
        assert_eq!(plain, b"ping", "upload cipher: {cipher:?}");

        let response_salt = vec![0x22_u8; cipher.salt_len()];
        let download_key = derive_download_key(cipher, &password, &response_salt).unwrap();
        let mut download_nonce = 0;
        let response =
            encrypt_tcp_chunk(cipher, &download_key, &mut download_nonce, b"pong").unwrap();
        server_io.write_all(&response_salt).await.unwrap();
        server_io.write_all(&response).await.unwrap();

        let mut buf = [0_u8; 4];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"pong", "download cipher: {cipher:?}");
    }
}

#[tokio::test]
async fn aead_stream_outbound_encrypts_upload_and_decrypts_download() {
    let cipher = CipherKind::Aes128Gcm;
    let password = b"test-password".to_vec();
    let upload_salt = [0x11_u8; 16];
    let upload_key = derive_key(&password, &upload_salt, cipher.key_len()).unwrap();
    let (client_io, mut server_io) = tokio::io::duplex(4096);

    let outbound_session = ShadowsocksOutboundSession {
        legacy: None,
        session_key: upload_key.clone(),
        next_upload_nonce: 0,
        cipher,
        request_salt: Vec::new(),
    };
    let mut stream = ShadowsocksAeadStream::outbound(client_io, outbound_session, password.clone());

    stream.write_all(b"ping").await.unwrap();
    stream.flush().await.unwrap();

    let mut encrypted_len = vec![0_u8; TCP_CHUNK_SIZE_LEN + cipher.tag_len()];
    server_io.read_exact(&mut encrypted_len).await.unwrap();
    let mut upload_nonce = 0;
    let payload_len =
        decrypt_tcp_chunk_length(cipher, &upload_key, &mut upload_nonce, &encrypted_len).unwrap();
    let mut encrypted_payload = vec![0_u8; payload_len + cipher.tag_len()];
    server_io.read_exact(&mut encrypted_payload).await.unwrap();
    let plain = decrypt_tcp_chunk_payload(
        cipher,
        &upload_key,
        &mut upload_nonce,
        payload_len,
        &encrypted_payload,
    )
    .unwrap();
    assert_eq!(plain, b"ping");

    let response_salt = [0x22_u8; 16];
    let download_key = derive_download_key(cipher, &password, &response_salt).unwrap();
    let mut download_nonce = 0;
    let response = encrypt_tcp_chunk(cipher, &download_key, &mut download_nonce, b"pong").unwrap();
    server_io.write_all(&response_salt).await.unwrap();
    server_io.write_all(&response).await.unwrap();

    let mut buf = [0_u8; 4];
    stream.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"pong");
}

#[tokio::test]
async fn aead_stream_inbound_serves_remaining_payload_and_encrypts_download() {
    let cipher = CipherKind::Aes128Gcm;
    let password = b"test-password";
    let request_salt = [0x33_u8; 16];
    let upload_key = derive_key(password, &request_salt, cipher.key_len()).unwrap();
    let response_salt = vec![0x44_u8; 16];
    let download_key = derive_download_key(cipher, password, &response_salt).unwrap();
    let (server_io, mut client_io) = tokio::io::duplex(4096);

    let mut stream = ShadowsocksAeadStream::inbound(
        server_io,
        cipher,
        upload_key,
        2,
        download_key.clone(),
        response_salt.clone(),
        b"first".to_vec(),
        false,
        Vec::new(),
    );

    let mut first = [0_u8; 5];
    stream.read_exact(&mut first).await.unwrap();
    assert_eq!(&first, b"first");

    stream.write_all(b"reply").await.unwrap();
    stream.flush().await.unwrap();

    let mut salt = vec![0_u8; response_salt.len()];
    client_io.read_exact(&mut salt).await.unwrap();
    assert_eq!(salt, response_salt);

    let mut encrypted_len = vec![0_u8; TCP_CHUNK_SIZE_LEN + cipher.tag_len()];
    client_io.read_exact(&mut encrypted_len).await.unwrap();
    let mut nonce = 0;
    let payload_len =
        decrypt_tcp_chunk_length(cipher, &download_key, &mut nonce, &encrypted_len).unwrap();
    let mut encrypted_payload = vec![0_u8; payload_len + cipher.tag_len()];
    client_io.read_exact(&mut encrypted_payload).await.unwrap();
    let plain = decrypt_tcp_chunk_payload(
        cipher,
        &download_key,
        &mut nonce,
        payload_len,
        &encrypted_payload,
    )
    .unwrap();
    assert_eq!(plain, b"reply");
}

#[tokio::test]
async fn accepted_inbound_stream_constructor_owns_response_key_derivation() {
    let cipher = CipherKind::Aes128Gcm;
    let password = b"test-password";
    let request_salt = [0x55_u8; 16];
    let upload_key = derive_key(password, &request_salt, cipher.key_len()).unwrap();
    let response_salt = vec![0x66_u8; 16];
    let download_key = derive_download_key(cipher, password, &response_salt).unwrap();
    let session = Session::new(
        0,
        Address::Domain("example.com".to_owned()),
        443,
        Network::Tcp,
        ProtocolType::new("shadowsocks"),
    );
    let accept = ShadowsocksAccept {
        legacy: None,
        session,
        remaining_payload: b"early".to_vec(),
        session_key: upload_key,
        cipher,
        next_upload_nonce: 2,
        request_salt: Vec::new(),
    };
    let (server_io, mut client_io) = tokio::io::duplex(4096);

    let mut stream = accept
        .into_aead_stream_with_response_salt(server_io, password, response_salt.clone())
        .expect("wrap accepted stream");

    let mut early = [0_u8; 5];
    stream.read_exact(&mut early).await.unwrap();
    assert_eq!(&early, b"early");

    stream.write_all(b"reply").await.unwrap();
    stream.flush().await.unwrap();

    let mut salt = vec![0_u8; response_salt.len()];
    client_io.read_exact(&mut salt).await.unwrap();
    assert_eq!(salt, response_salt);

    let mut encrypted_len = vec![0_u8; TCP_CHUNK_SIZE_LEN + cipher.tag_len()];
    client_io.read_exact(&mut encrypted_len).await.unwrap();
    let mut nonce = 0;
    let payload_len =
        decrypt_tcp_chunk_length(cipher, &download_key, &mut nonce, &encrypted_len).unwrap();
    let mut encrypted_payload = vec![0_u8; payload_len + cipher.tag_len()];
    client_io.read_exact(&mut encrypted_payload).await.unwrap();
    let plain = decrypt_tcp_chunk_payload(
        cipher,
        &download_key,
        &mut nonce,
        payload_len,
        &encrypted_payload,
    )
    .unwrap();
    assert_eq!(plain, b"reply");
}

#[test]
fn udp_datagram_framing_roundtrips_all_supported_ciphers() {
    for cipher in supported_ciphers() {
        let password = password_for_cipher(cipher);
        let datagram = <ShadowsocksOutbound as UdpDatagramFraming<
            ShadowsocksUdpPacketTarget,
            ShadowsocksUdpDecodeContext,
        >>::encode_udp_datagram(
            &ShadowsocksOutbound,
            &ShadowsocksUdpPacketTarget {
                target: &Address::Domain("dns.google".to_owned()),
                port: 53,
                payload: b"query",
                cipher,
                password,
            },
        )
        .expect("encode udp datagram");

        assert!(datagram.len() > cipher.udp_salt_len() + cipher.tag_len());

        let decoded = <ShadowsocksOutbound as UdpDatagramFraming<
            ShadowsocksUdpPacketTarget,
            ShadowsocksUdpDecodeContext,
        >>::decode_udp_datagram(
            &ShadowsocksOutbound,
            &ShadowsocksUdpDecodeContext { cipher, password },
            &datagram,
        )
        .expect("decode udp datagram");

        assert_eq!(decoded.target(), &Address::Domain("dns.google".to_owned()));
        assert_eq!(decoded.port(), 53);
        assert_eq!(decoded.payload(), b"query", "cipher: {cipher:?}");
    }
}

#[test]
fn udp_datagram_framing_roundtrips_aead_packet() {
    let cipher = CipherKind::Aes128Gcm;
    let password = b"test-password";

    let datagram = <ShadowsocksOutbound as UdpDatagramFraming<
        ShadowsocksUdpPacketTarget,
        ShadowsocksUdpDecodeContext,
    >>::encode_udp_datagram(
        &ShadowsocksOutbound,
        &ShadowsocksUdpPacketTarget {
            target: &Address::Ipv4([8, 8, 8, 8]),
            port: 53,
            payload: b"query",
            cipher,
            password,
        },
    )
    .expect("encode udp datagram");

    assert!(datagram.len() > cipher.udp_salt_len() + cipher.tag_len());

    let decoded = <ShadowsocksOutbound as UdpDatagramFraming<
        ShadowsocksUdpPacketTarget,
        ShadowsocksUdpDecodeContext,
    >>::decode_udp_datagram(
        &ShadowsocksOutbound,
        &ShadowsocksUdpDecodeContext { cipher, password },
        &datagram,
    )
    .expect("decode udp datagram");

    assert_eq!(decoded.target(), &Address::Ipv4([8, 8, 8, 8]));
    assert_eq!(decoded.port(), 53);
    assert_eq!(decoded.payload(), b"query");
}

#[test]
fn udp_datagram_helpers_roundtrip_aead_packet() {
    let cipher = CipherKind::Aes128Gcm;
    let password = b"test-password";
    let codec = ShadowsocksDatagramCodec::new(cipher, password);

    let datagram = codec
        .encode(&Address::Domain("dns.google".to_owned()), 53, b"query")
        .expect("encode udp datagram");

    let decoded = codec.decode(&datagram).expect("decode udp datagram");

    assert_eq!(decoded.0, Address::Domain("dns.google".to_owned()));
    assert_eq!(decoded.1, 53);
    assert_eq!(decoded.2, b"query");
}

#[cfg(feature = "blake3")]
#[test]
fn udp_datagram_framing_roundtrips_2022_blake3_packet() {
    let cipher = CipherKind::Blake3Aes128Gcm;
    let password = password_for_cipher(cipher);

    let datagram = <ShadowsocksOutbound as UdpDatagramFraming<
        ShadowsocksUdpPacketTarget,
        ShadowsocksUdpDecodeContext,
    >>::encode_udp_datagram(
        &ShadowsocksOutbound,
        &ShadowsocksUdpPacketTarget {
            target: &Address::Domain("dns.google".to_owned()),
            port: 53,
            payload: b"query",
            cipher,
            password,
        },
    )
    .expect("encode udp datagram");

    let decoded = <ShadowsocksOutbound as UdpDatagramFraming<
        ShadowsocksUdpPacketTarget,
        ShadowsocksUdpDecodeContext,
    >>::decode_udp_datagram(
        &ShadowsocksOutbound,
        &ShadowsocksUdpDecodeContext { cipher, password },
        &datagram,
    )
    .expect("decode udp datagram");

    assert_eq!(decoded.target(), &Address::Domain("dns.google".to_owned()));
    assert_eq!(decoded.port(), 53);
    assert_eq!(decoded.payload(), b"query");
}

/// SIP022 3.2.3 server-to-client UDP response: a server recovers the client
/// session id from an incoming request and echoes it in the response body so
/// the client can map the response. Exercises the full server flow for all
/// three 2022 ciphers (AES separate-header + ChaCha20 nonce variants).
#[cfg(feature = "blake3")]
#[test]
fn udp_2022_server_response_flow_all_blake3_ciphers() {
    for cipher in blake3_ciphers() {
        let password = password_for_cipher(cipher);
        let target = Address::Domain("dns.google".to_owned());
        let client_codec = ShadowsocksDatagramCodec::new(cipher, password);
        let profile =
            ShadowsocksInboundProfile::from_config(cipher_name(cipher), password_str(cipher))
                .expect("build inbound profile");
        let mut server_session = profile.udp_session();

        // Client -> server request.
        let request = client_codec
            .encode(&target, 53, b"query")
            .expect("encode client request");
        // Server decodes and recovers the client session id.
        let decoded_request = server_session
            .decode_request(&request)
            .expect("decode client request");
        assert_eq!(decoded_request.target(), &target);
        assert_eq!(decoded_request.port(), 53);
        assert_eq!(decoded_request.payload(), b"query");
        let client_session_id = decoded_request
            .client_session_id()
            .expect("2022 request should carry a session id");
        assert_ne!(client_session_id, 0, "client session id must be non-zero");

        // Server -> client response echoing the client session id.
        let response = server_session
            .encode_response_to_client(Some(client_session_id), &target, 53, b"answer")
            .expect("encode server response")
            .into_datagram();
        // Only the client association accepts a response; the server rejects it.
        assert!(server_session.decode_request(&response).is_err());
        let (response_target, response_port, response_payload) = client_codec
            .decode(&response)
            .expect("decode server response");
        assert_eq!(response_target, target);
        assert_eq!(response_port, 53);
        assert_eq!(response_payload, b"answer");
        assert!(
            client_codec.decode(&response).is_none(),
            "duplicate response must be rejected"
        );
        // A response datagram (type 1) is 8 bytes larger than the equivalent
        // client datagram (type 0) because of the echoed client session id.
        let baseline = client_codec.encode(&target, 53, b"answer").unwrap();
        assert_eq!(
            response.len(),
            baseline.len() + 8,
            "server response must carry the echoed client session id cipher: {cipher:?}"
        );
    }
}

/// SIP022 3.1.3 detection prevention: a probe that sends fewer bytes than the
/// salt + fixed-length header is rejected, and the drain does not hang when
/// the peer then closes the connection.
#[cfg(feature = "blake3")]
#[tokio::test]
async fn ss_2022_rejects_short_header_probe() {
    let cipher = CipherKind::Blake3Aes256Gcm;
    let password = password_for_cipher(cipher).to_vec();
    let (client_io, server_io) = tokio::io::duplex(8192);
    let (mut client_io, mut server_io) = (TestSocket(client_io), TestSocket(server_io));

    // Send far fewer bytes than salt(32) + fixed-header(27), then close.
    AsyncWriteExt::write_all(&mut client_io, &[0u8; 5])
        .await
        .unwrap();
    AsyncWriteExt::shutdown(&mut client_io).await.unwrap();

    let accept = ShadowsocksInbound
        .accept_request(&mut server_io, cipher, &password)
        .await;
    assert!(
        accept.is_err(),
        "a probe with too few bytes must be rejected"
    );
}

// ---- 2022 edition (SIP022) TCP ----
//
// The request stream is salt + fixed-header chunk (nonce 0) + variable-header
// chunk (nonce 1) + body length/payload pairs; the response stream is salt +
// fixed-header chunk (nonce 0, doubling as first length chunk) + payload chunk
// (nonce 1) + body length/payload pairs. These tests drive the full
// send_request / accept_request / into_aead_stream pipeline for all three
// 2022 ciphers.

/// Wraps a tokio `DuplexStream` so it satisfies both `AsyncSocket` (needed by
/// `send_request` / `accept_request`) and `AsyncRead` + `AsyncWrite` (needed by
/// `ShadowsocksAeadStream`).
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

// ---- 2022 edition (SIP022) TCP ----
//
// The request stream is salt + fixed-header chunk (nonce 0) + variable-header
// chunk (nonce 1) + body length/payload pairs; the response stream is salt +
// fixed-header chunk (nonce 0, doubling as first length chunk) + payload chunk
// (nonce 1) + body length/payload pairs. These tests drive the full
// send_request / accept_request / into_aead_stream pipeline for all three
// 2022 ciphers.

#[cfg(feature = "blake3")]
#[tokio::test]
async fn ss_2022_tcp_request_and_accept_roundtrip_all_blake3_ciphers() {
    for cipher in blake3_ciphers() {
        let password = password_for_cipher(cipher).to_vec();
        let (client_io, server_io) = tokio::io::duplex(8192);
        let (mut client_io, mut server_io) = (TestSocket(client_io), TestSocket(server_io));

        let session = Session::new(
            0,
            Address::Domain("example.com".to_owned()),
            443,
            Network::Tcp,
            ProtocolType::new("shadowsocks"),
        );

        // Client writes: salt + fixed header (nonce 0) + var header (nonce 1).
        let outbound_session = ShadowsocksOutbound
            .send_request(&mut client_io, &session, cipher, &password)
            .await
            .expect("send 2022 request");
        assert_eq!(outbound_session.next_upload_nonce, 2, "cipher: {cipher:?}");
        assert_eq!(
            outbound_session.request_salt.len(),
            cipher.salt_len(),
            "cipher: {cipher:?}"
        );

        // Server reads the same bytes back and parses the target.
        let accept = ShadowsocksInbound
            .accept_request(&mut server_io, cipher, &password)
            .await
            .expect("accept 2022 request");
        assert_eq!(
            accept.session.target,
            Address::Domain("example.com".to_owned())
        );
        assert_eq!(accept.session.port, 443);
        assert_eq!(accept.next_upload_nonce, 2, "cipher: {cipher:?}");
        assert_eq!(accept.request_salt, outbound_session.request_salt);
        // No initial payload was carried, so nothing is buffered.
        assert!(accept.remaining_payload.is_empty());
    }
}

#[cfg(feature = "blake3")]
#[tokio::test]
async fn ss_2022_tcp_full_relay_roundtrips_all_blake3_ciphers() {
    for cipher in blake3_ciphers() {
        let password = password_for_cipher(cipher).to_vec();
        let (client_io, server_io) = tokio::io::duplex(8192);
        let (mut client_io, mut server_io) = (TestSocket(client_io), TestSocket(server_io));

        let session = Session::new(
            0,
            Address::Domain("example.com".to_owned()),
            443,
            Network::Tcp,
            ProtocolType::new("shadowsocks"),
        );

        // send_request / accept_request borrow the transports; afterwards we
        // move them into the AEAD stream wrappers for the relay.
        let outbound_session = ShadowsocksOutbound
            .send_request(&mut client_io, &session, cipher, &password)
            .await
            .expect("send 2022 request");
        let accept = ShadowsocksInbound
            .accept_request(&mut server_io, cipher, &password)
            .await
            .expect("accept 2022 request");

        let mut server_stream = accept
            .into_aead_stream(server_io, &password)
            .expect("wrap server stream");
        let mut client_stream =
            ShadowsocksAeadStream::outbound(client_io, outbound_session, password.clone());

        // Upload: client -> server (body length/payload pairs from nonce 2).
        client_stream.write_all(b"ping").await.unwrap();
        client_stream.flush().await.unwrap();
        let mut up = [0u8; 4];
        server_stream.read_exact(&mut up).await.unwrap();
        assert_eq!(&up, b"ping", "upload cipher: {cipher:?}");

        // Download: server -> client. The first write emits the response salt
        // + fixed header (nonce 0) + first payload chunk (nonce 1); the client
        // verifies the echoed request salt and timestamp.
        server_stream.write_all(b"pong").await.unwrap();
        server_stream.flush().await.unwrap();
        let mut down = [0u8; 4];
        client_stream.read_exact(&mut down).await.unwrap();
        assert_eq!(&down, b"pong", "download cipher: {cipher:?}");
    }
}

#[cfg(feature = "blake3")]
#[tokio::test]
async fn ss_2022_tcp_relay_large_payload_spans_multiple_chunks() {
    // A payload larger than one chunk (0x3FFF) exercises multi-chunk body
    // framing in both directions, including the 2022 response header acting as
    // the first length chunk followed by further length/payload pairs.
    let cipher = CipherKind::Blake3Aes256Gcm;
    let password = password_for_cipher(cipher).to_vec();
    let (client_io, server_io) = tokio::io::duplex(1 << 16);
    let (mut client_io, mut server_io) = (TestSocket(client_io), TestSocket(server_io));

    let session = Session::new(
        0,
        Address::Ipv4([93, 184, 216, 34]),
        80,
        Network::Tcp,
        ProtocolType::new("shadowsocks"),
    );

    let outbound_session = ShadowsocksOutbound
        .send_request(&mut client_io, &session, cipher, &password)
        .await
        .unwrap();
    let accept = ShadowsocksInbound
        .accept_request(&mut server_io, cipher, &password)
        .await
        .unwrap();
    assert_eq!(accept.session.target, Address::Ipv4([93, 184, 216, 34]));

    let mut server_stream = accept
        .into_aead_stream(server_io, &password)
        .expect("wrap server stream");
    let mut client_stream =
        ShadowsocksAeadStream::outbound(client_io, outbound_session, password.clone());

    let payload: Vec<u8> = (0..40_000u32).map(|i| (i & 0xff) as u8).collect();

    client_stream.write_all(&payload).await.unwrap();
    client_stream.flush().await.unwrap();
    let mut received = vec![0u8; payload.len()];
    server_stream.read_exact(&mut received).await.unwrap();
    assert_eq!(received, payload, "upload multi-chunk");

    server_stream.write_all(&payload).await.unwrap();
    server_stream.flush().await.unwrap();
    let mut echoed = vec![0u8; payload.len()];
    client_stream.read_exact(&mut echoed).await.unwrap();
    assert_eq!(echoed, payload, "download multi-chunk");
}

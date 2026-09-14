//! Generic TLS 1.3 client handshake — no REALITY-specific auth.
//!
//! Based on the REALITY client connection state machine with
//! REALITY-specific parts replaced by standard TLS 1.3 behavior:
//! - Random session ID (no REALITY metadata encryption)
//! - Standard webpki certificate chain verification
//! - Standard ECDSA/RSA CertificateVerify verification

use std::io::{self, Read, Write};
use std::sync::Arc;
use std::time::Instant;

use rand::RngCore;
use ring::digest;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::client::WebPkiServerVerifier;
use rustls::internal::msgs::codec::{Codec, Reader};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, RootCertStore};
use subtle::ConstantTimeEq;
use x25519_dalek::{PublicKey, StaticSecret};

use crate::aead::{decrypt_handshake_message, AeadKey};
use crate::cipher::CipherSuite;
use crate::common::{
    ALERT_DESC_CLOSE_NOTIFY, ALERT_LEVEL_WARNING, CIPHERTEXT_READ_BUF_CAPACITY, CONTENT_TYPE_ALERT,
    CONTENT_TYPE_APPLICATION_DATA, CONTENT_TYPE_CHANGE_CIPHER_SPEC, CONTENT_TYPE_HANDSHAKE,
    HANDSHAKE_TYPE_CERTIFICATE, HANDSHAKE_TYPE_CERTIFICATE_VERIFY,
    HANDSHAKE_TYPE_ENCRYPTED_EXTENSIONS, HANDSHAKE_TYPE_FINISHED, OUTGOING_BUFFER_LIMIT,
    PLAINTEXT_READ_BUF_CAPACITY, TLS_MAX_RECORD_SIZE, TLS_RECORD_HEADER_SIZE,
};
use crate::fingerprint::ClientHelloProfile;
use crate::keys::{
    compute_finished_verify_data, derive_application_secrets, derive_handshake_keys,
    derive_traffic_keys,
};
use crate::messages::{construct_finished, write_record_header};
use crate::record::{RecordDecryptor, RecordEncryptor};
use crate::slide_buffer::SlideBuffer;
mod extensions;
mod hello_retry;
mod server_hello;

/// Configuration for a generic TLS 1.3 client.
#[derive(Clone)]
pub struct Tls13Config {
    /// Server name for SNI.
    pub server_name: String,
    /// TLS 1.3 cipher suites in preference order.
    pub cipher_suites: Vec<CipherSuite>,
    /// ALPN protocols (default: ["h2", "http/1.1"]).
    pub alpn_protocols: Vec<String>,
    /// Use the browser preset ALPN when no explicit protocols are supplied.
    pub use_profile_alpn: bool,
    /// Browser-family ClientHello template.
    pub client_hello_profile: ClientHelloProfile,
    /// Explicit wire overrides; generic TLS only advertises TLS 1.3.
    pub client_hello_options: crate::fingerprint::wire::ClientHelloOptions,
    /// Group implementations supplied by the owning transport for custom curves.
    pub key_exchange_provider: Option<Arc<rustls::crypto::CryptoProvider>>,
    /// Certificate verification policy. `None` uses the public WebPKI roots.
    pub server_verifier: Option<Arc<dyn ServerCertVerifier>>,
    /// Handshake timeout in milliseconds.
    pub handshake_timeout_ms: u64,
}

fn default_server_verifier() -> io::Result<Arc<WebPkiServerVerifier>> {
    let roots = RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    WebPkiServerVerifier::builder(Arc::new(roots))
        .build()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error.to_string()))
}

fn verify_server_certificate(
    verifier: &dyn ServerCertVerifier,
    certificates: &[CertificateDer<'static>],
    server_name: &str,
) -> io::Result<ServerCertVerified> {
    let (end_entity, intermediates) = certificates
        .split_first()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "empty certificate chain"))?;
    let server_name = ServerName::try_from(server_name.to_owned())
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error.to_string()))?;
    verifier
        .verify_server_cert(
            end_entity,
            intermediates,
            &server_name,
            &[],
            UnixTime::now(),
        )
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, format!("{error:?}")))
}

fn verify_server_signature(
    verifier: &dyn ServerCertVerifier,
    message: &[u8],
    certificate: &CertificateDer<'_>,
    signed: &DigitallySignedStruct,
) -> io::Result<HandshakeSignatureValid> {
    verifier
        .verify_tls13_signature(message, certificate, signed)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))
}

fn parse_certificate_chain(message: &[u8]) -> io::Result<Vec<CertificateDer<'static>>> {
    crate::certificate::server_chain(message).map(|chain| {
        chain
            .into_iter()
            .map(|der| CertificateDer::from(der.to_vec()))
            .collect()
    })
}

fn parse_certificate_verify(message: &[u8]) -> io::Result<DigitallySignedStruct> {
    if message.len() < 8 || message[0] != HANDSHAKE_TYPE_CERTIFICATE_VERIFY {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid CertificateVerify message",
        ));
    }
    let body_len = read_u24(&message[1..4]);
    if body_len + 4 > message.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "truncated CertificateVerify message",
        ));
    }
    let signature_len = u16::from_be_bytes([message[6], message[7]]) as usize;
    if signature_len + 8 != body_len + 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid CertificateVerify signature length",
        ));
    }
    let mut reader = Reader::init(&message[4..4 + body_len]);
    DigitallySignedStruct::read(&mut reader)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, format!("{error:?}")))
}

fn tls13_server_certificate_verify_message(transcript_hash: &[u8]) -> Vec<u8> {
    let mut message = vec![0x20; 64];
    message.extend_from_slice(b"TLS 1.3, server CertificateVerify");
    message.push(0);
    message.extend_from_slice(transcript_hash);
    message
}

fn verify_server_finished(
    cipher_suite: CipherSuite,
    server_hs_secret: &[u8],
    transcript_prefix: &[u8],
    handshake_messages: &[u8],
    finished_offset: usize,
) -> io::Result<()> {
    let finished = &handshake_messages[finished_offset..];
    if finished.len() < 4 || finished[0] != HANDSHAKE_TYPE_FINISHED {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid Finished message",
        ));
    }
    let verify_len = read_u24(&finished[1..4]);
    if finished.len() < 4 + verify_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "truncated Finished verify_data",
        ));
    }
    let mut transcript = digest::Context::new(cipher_suite.digest_algorithm());
    transcript.update(transcript_prefix);
    transcript.update(&handshake_messages[..finished_offset]);
    let transcript_hash = transcript.finish();
    let expected =
        compute_finished_verify_data(cipher_suite, server_hs_secret, transcript_hash.as_ref())?;
    if expected
        .as_slice()
        .ct_eq(&finished[4..4 + verify_len])
        .into()
    {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid server Finished",
        ))
    }
}

fn find_handshake_message(messages: &[u8], expected_type: u8) -> Option<usize> {
    let mut offset = 0;
    while offset + 4 <= messages.len() {
        let message_len = read_u24(&messages[offset + 1..offset + 4]);
        if offset + 4 + message_len > messages.len() {
            return None;
        }
        if messages[offset] == expected_type {
            return Some(offset);
        }
        offset += 4 + message_len;
    }
    None
}

fn read_u24(bytes: &[u8]) -> usize {
    ((bytes[0] as usize) << 16) | ((bytes[1] as usize) << 8) | bytes[2] as usize
}

impl Default for Tls13Config {
    fn default() -> Self {
        Self {
            server_name: String::new(),
            cipher_suites: Vec::new(),
            alpn_protocols: Vec::new(),
            use_profile_alpn: true,
            client_hello_profile: ClientHelloProfile::DEFAULT,
            client_hello_options: crate::fingerprint::wire::ClientHelloOptions {
                tls13_only: true,
                ..Default::default()
            },
            key_exchange_provider: None,
            server_verifier: None,
            handshake_timeout_ms: 10_000,
        }
    }
}

// ── Handshake states ────────────────────────────────────────────────

enum State {
    /// ClientHello sent, waiting for ServerHello.
    AwaitingServerHello {
        client_hello_bytes: Vec<u8>,
        client_private_key: [u8; 32],
        extra_keys: crate::fingerprint::key_share::KeyShares,
        retry_transcript: Vec<u8>,
        retry_suite: Option<u16>,
        server_hello_fragments: Vec<u8>,
    },
    /// ServerHello received, processing encrypted handshake messages.
    ProcessingHandshake {
        client_hs_secret: Vec<u8>,
        server_hs_secret: Vec<u8>,
        master_secret: Vec<u8>,
        cipher_suite: CipherSuite,
        transcript_bytes: Vec<u8>,
        handshake_seq: u64,
        accumulated_plaintext: Vec<u8>,
    },
    /// Handshake complete.
    Complete,
}

// ── Connection ──────────────────────────────────────────────────────

/// A generic TLS 1.3 client connection.
///
/// Provides rustls-compatible `read_tls` / `write_tls` / `process_new_packets`
/// for integration with async I/O.
pub struct Tls13Connection {
    negotiated_alpn: Option<Vec<u8>>,
    offered_alpn: Vec<String>,
    tickets: crate::post_handshake::TicketSink,
    read_secret: Option<crate::post_handshake::traffic::TrafficSecret>,
    write_secret: Option<crate::post_handshake::traffic::TrafficSecret>,
    config: Tls13Config,
    state: State,

    // Post-handshake keys
    app_read_key: Option<AeadKey>,
    app_read_iv: Option<Vec<u8>>,
    app_write_key: Option<AeadKey>,
    app_write_iv: Option<Vec<u8>>,
    read_seq: u64,
    write_seq: u64,

    // I/O buffers
    tls_read_buf: Box<[u8]>,
    ciphertext_read_buf: SlideBuffer,
    ciphertext_write_buf: Vec<u8>,
    plaintext_read_buf: SlideBuffer,
    plaintext_write_buf: Vec<u8>,

    received_close_notify: bool,
    fatal_error: Option<io::ErrorKind>,
    handshake_start: Instant,
}

impl Tls13Connection {
    /// Create a new TLS 1.3 client and build the ClientHello.
    pub fn new(mut config: Tls13Config) -> io::Result<Self> {
        if config.client_hello_profile == ClientHelloProfile::Random {
            config.client_hello_profile =
                crate::fingerprint::wire::resolve(config.client_hello_profile);
        }
        let mut conn = Self {
            config,
            state: State::AwaitingServerHello {
                client_hello_bytes: Vec::new(),
                client_private_key: [0u8; 32],
                extra_keys: Default::default(),
                retry_transcript: Vec::new(),
                retry_suite: None,
                server_hello_fragments: Vec::new(),
            },
            negotiated_alpn: None,
            offered_alpn: Vec::new(),
            tickets: Default::default(),
            read_secret: None,
            write_secret: None,
            app_read_key: None,
            app_read_iv: None,
            app_write_key: None,
            app_write_iv: None,
            read_seq: 0,
            write_seq: 0,
            tls_read_buf: vec![0u8; TLS_MAX_RECORD_SIZE].into_boxed_slice(),
            ciphertext_read_buf: SlideBuffer::new(CIPHERTEXT_READ_BUF_CAPACITY),
            ciphertext_write_buf: Vec::with_capacity(OUTGOING_BUFFER_LIMIT),
            plaintext_read_buf: SlideBuffer::new(PLAINTEXT_READ_BUF_CAPACITY),
            plaintext_write_buf: Vec::with_capacity(OUTGOING_BUFFER_LIMIT),
            received_close_notify: false,
            fatal_error: None,
            handshake_start: Instant::now(),
        };
        conn.build_client_hello()?;
        Ok(conn)
    }

    fn build_client_hello(&mut self) -> io::Result<()> {
        let mut rng = rand::rng();
        let mut our_private_bytes = [0u8; 32];
        rng.fill_bytes(&mut our_private_bytes);

        let our_private_key = StaticSecret::from(our_private_bytes);
        let our_public_key = PublicKey::from(&our_private_key);

        let mut client_random = [0u8; 32];
        rng.fill_bytes(&mut client_random);

        // Standard random session ID — no REALITY metadata
        let mut session_id = [0u8; 32];
        rng.fill_bytes(&mut session_id);

        let cipher_suite_ids: Vec<u16> =
            self.config.cipher_suites.iter().map(|cs| cs.id()).collect();
        let alpn_strs: Vec<&str> = self
            .config
            .alpn_protocols
            .iter()
            .map(|s| s.as_str())
            .collect();
        let alpn_refs = if alpn_strs.is_empty() && self.config.use_profile_alpn {
            self.config.client_hello_profile.alpn_protocols()
        } else {
            &alpn_strs
        };

        let mut extra_keys =
            crate::fingerprint::key_share::KeyShares::for_profile(self.config.client_hello_profile)
                .with_provider(
                    self.config.key_exchange_provider.clone(),
                    !self.config.client_hello_options.supported_groups.is_empty(),
                );
        let client_hello = crate::fingerprint::wire::build_with_options(
            &client_random,
            &session_id,
            &self.config.server_name,
            &cipher_suite_ids,
            alpn_refs,
            self.config.client_hello_profile,
            &self.config.client_hello_options,
            |group| extra_keys.offer(group, our_public_key.as_bytes()),
        )?;

        self.offered_alpn = extensions::offered_alpn(&client_hello)?;
        let mut record = write_record_header(CONTENT_TYPE_HANDSHAKE, client_hello.len() as u16);
        record[1..3].copy_from_slice(&[3, 1]); // uTLS initial ClientHello record version
        record.extend_from_slice(&client_hello);
        self.ciphertext_write_buf.extend_from_slice(&record);

        self.state = State::AwaitingServerHello {
            client_hello_bytes: client_hello,
            client_private_key: our_private_bytes,
            extra_keys,
            retry_transcript: Vec::new(),
            retry_suite: None,
            server_hello_fragments: Vec::new(),
        };
        Ok(())
    }

    // ── Public API ──────────────────────────────────────────────────

    pub fn alpn_protocol(&self) -> Option<&[u8]> {
        self.negotiated_alpn.as_deref()
    }

    pub(crate) fn buffered_ciphertext_len(&self) -> usize {
        self.ciphertext_read_buf.len()
    }

    pub(crate) fn next_record_read_size(&self) -> io::Result<usize> {
        let buffered = self.ciphertext_read_buf.len();
        if buffered < TLS_RECORD_HEADER_SIZE {
            return Ok(TLS_RECORD_HEADER_SIZE - buffered);
        }
        let length = self.ciphertext_read_buf.get_u16_be(3).unwrap() as usize;
        if length > 18432 {
            return Err(io::Error::other("TLS record exceeds limit"));
        }
        (TLS_RECORD_HEADER_SIZE + length)
            .checked_sub(buffered)
            .filter(|n| *n > 0)
            .ok_or_else(|| io::Error::other("unconsumed TLS record"))
    }

    pub fn read_tls(&mut self, rd: &mut dyn Read) -> io::Result<usize> {
        if self.ciphertext_read_buf.remaining_capacity() < TLS_MAX_RECORD_SIZE {
            self.ciphertext_read_buf.compact();
        }
        let n = rd.read(&mut self.tls_read_buf[..])?;
        if n > 0 {
            self.ciphertext_read_buf
                .extend_from_slice(&self.tls_read_buf[..n]);
        }
        Ok(n)
    }

    pub fn write_tls(&mut self, wr: &mut dyn Write) -> io::Result<usize> {
        // Encrypt pending plaintext
        if matches!(self.state, State::Complete) && !self.plaintext_write_buf.is_empty() {
            if let (Some(key), Some(iv)) = (&self.app_write_key, &self.app_write_iv) {
                let mut enc = RecordEncryptor::new(key, iv, &mut self.write_seq);
                enc.encrypt_app_data(
                    &mut self.plaintext_write_buf,
                    &mut self.ciphertext_write_buf,
                )?;
            }
        }
        let n = wr.write(&self.ciphertext_write_buf)?;
        self.ciphertext_write_buf.drain(..n);
        Ok(n)
    }

    pub fn process_new_packets(&mut self) -> io::Result<usize> {
        if let Some(kind) = self.fatal_error {
            return Err(kind.into());
        }
        let result = self.process_packets_inner();
        if let Err(error) = &result {
            self.fatal_error = Some(error.kind());
        }
        result
    }
    fn process_packets_inner(&mut self) -> io::Result<usize> {
        loop {
            match &self.state {
                State::AwaitingServerHello { .. } => {
                    if !self.process_server_hello()? {
                        break;
                    }
                }
                State::ProcessingHandshake { .. } => {
                    if !self.process_encrypted_handshake()? {
                        break;
                    }
                }
                State::Complete => {
                    self.process_app_data()?;
                    break;
                }
            }
        }
        Ok(self.plaintext_read_buf.len())
    }

    pub fn wants_write(&self) -> bool {
        !self.ciphertext_write_buf.is_empty() || !self.plaintext_write_buf.is_empty()
    }

    pub fn is_handshaking(&self) -> bool {
        !matches!(self.state, State::Complete)
    }

    pub fn wants_read(&self) -> bool {
        if self.received_close_notify || self.fatal_error.is_some() {
            return false;
        }
        if self.is_handshaking() {
            return true;
        }
        self.plaintext_read_buf.is_empty()
    }

    /// Get plaintext application data from the read buffer.
    pub fn take_plaintext(&mut self) -> Option<Vec<u8>> {
        if self.plaintext_read_buf.is_empty() {
            return None;
        }
        let len = self.plaintext_read_buf.len();
        let data = self.plaintext_read_buf[..len].to_vec();
        self.plaintext_read_buf.consume(len);
        Some(data)
    }

    /// Copy decrypted application data into `dst`, consuming only what was
    /// copied. Returns the number of bytes copied (0 when nothing is
    /// buffered). Unlike [`take_plaintext`](Self::take_plaintext), this never
    /// discards data that does not fit, so it is safe to use with small
    /// caller-provided read buffers.
    pub fn read_plaintext(&mut self, dst: &mut [u8]) -> usize {
        let src = self.plaintext_read_buf.as_slice();
        let n = src.len().min(dst.len());
        dst[..n].copy_from_slice(&src[..n]);
        self.plaintext_read_buf.consume(n);
        n
    }

    /// Number of decrypted application-data bytes available to read.
    pub fn plaintext_len(&self) -> usize {
        self.plaintext_read_buf.len()
    }

    /// Queue plaintext for encryption and sending.
    pub fn write_plaintext(&mut self, data: &[u8]) {
        self.plaintext_write_buf.extend_from_slice(data);
    }

    // ── Handshake processing ────────────────────────────────────────

    fn process_server_hello(&mut self) -> io::Result<bool> {
        let State::AwaitingServerHello {
            client_hello_bytes,
            client_private_key,
            extra_keys,
            retry_transcript,
            retry_suite,
            server_hello_fragments,
        } = &mut self.state
        else {
            unreachable!()
        };

        // Check timeout
        if self.handshake_start.elapsed().as_millis() > self.config.handshake_timeout_ms as u128 {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "TLS 1.3 handshake timed out",
            ));
        }

        if self.ciphertext_read_buf.len() < TLS_RECORD_HEADER_SIZE {
            return Ok(false);
        }
        let record_len = self
            .ciphertext_read_buf
            .get_u16_be(3)
            .ok_or_else(|| io::Error::other("buffer too short"))? as usize;
        let total = TLS_RECORD_HEADER_SIZE + record_len;
        if self.ciphertext_read_buf.len() < total {
            return Ok(false);
        }

        let record: Vec<u8> = self.ciphertext_read_buf[..total].to_vec();
        self.ciphertext_read_buf.consume(total);
        let Some(record) = server_hello::assemble(server_hello_fragments, &record)? else {
            return Ok(true);
        };
        let hello = hello_retry::parse(&record, client_hello_bytes)?;
        let cipher_suite = CipherSuite::from_id(hello.suite)
            .ok_or_else(|| io::Error::other("unsupported TLS 1.3 cipher suite"))?;
        if hello.retry {
            if retry_suite.is_some() {
                return Err(io::Error::other("repeated HelloRetryRequest"));
            }
            let hash = digest::digest(cipher_suite.digest_algorithm(), client_hello_bytes);
            retry_transcript.extend_from_slice(&[254, 0, 0, hash.as_ref().len() as u8]);
            retry_transcript.extend_from_slice(hash.as_ref());
            retry_transcript.extend_from_slice(&record[TLS_RECORD_HEADER_SIZE..]);
            let public = PublicKey::from(&StaticSecret::from(*client_private_key));
            *client_hello_bytes =
                hello_retry::rebuild(client_hello_bytes, &hello, extra_keys, public.as_bytes())?;
            *retry_suite = Some(hello.suite);
            for fragment in client_hello_bytes.chunks(16384) {
                self.ciphertext_write_buf
                    .extend_from_slice(&write_record_header(
                        CONTENT_TYPE_HANDSHAKE,
                        fragment.len() as u16,
                    ));
                self.ciphertext_write_buf.extend_from_slice(fragment);
            }
            return Ok(true);
        }
        if retry_suite.is_some_and(|suite| suite != hello.suite) {
            return Err(io::Error::other(
                "ServerHello changed the retry cipher suite",
            ));
        }
        let (group, server_public_key) = crate::fingerprint::key_share::selected(&record)?;
        if !hello_retry::offered_share(client_hello_bytes, group)? {
            return Err(io::Error::other(
                "ServerHello selected an unoffered key share",
            ));
        }
        let client_hello_bytes = client_hello_bytes.clone();
        // Compute the cumulative transcript hash Hash(ClientHello || ServerHello).
        //
        // Per RFC 8446 §7.1, the handshake traffic secrets are bound to
        // Transcript-Hash(ClientHello..ServerHello) — the *running* hash over
        // both messages — not Hash(ServerHello) alone. Using the latter
        // produces keys that no standard TLS 1.3 server (rustls, etc.) will
        // accept, so the AEAD open fails with an auth-tag mismatch. The
        // REALITY client (reality_client_connection.rs) already does this
        // correctly; this path must match.
        let server_hello = &record[TLS_RECORD_HEADER_SIZE..];
        let mut transcript = digest::Context::new(cipher_suite.digest_algorithm());
        transcript.update(retry_transcript);
        transcript.update(&client_hello_bytes);
        transcript.update(server_hello);
        let transcript_hash = transcript.finish();

        let shared_secret = if group == 29 && !extra_keys.reuses_x25519() {
            let peer = PublicKey::from(
                <[u8; 32]>::try_from(server_public_key.as_slice())
                    .map_err(|_| io::Error::other("invalid X25519 key"))?,
            );
            let private = StaticSecret::from(*client_private_key);
            let secret = private.diffie_hellman(&peer);
            if !secret.was_contributory() {
                return Err(io::Error::other("non-contributory X25519 key"));
            }
            secret.as_bytes().to_vec()
        } else {
            extra_keys.complete(group, &server_public_key)?
        };

        let hs_keys =
            derive_handshake_keys(cipher_suite, &shared_secret, &[], transcript_hash.as_ref())?;

        // Build transcript (raw bytes, not hashes)
        let mut transcript_bytes = retry_transcript.clone();
        transcript_bytes.extend_from_slice(&client_hello_bytes);
        transcript_bytes.extend_from_slice(server_hello);

        self.state = State::ProcessingHandshake {
            client_hs_secret: hs_keys.client_handshake_traffic_secret.clone(),
            server_hs_secret: hs_keys.server_handshake_traffic_secret.clone(),
            master_secret: hs_keys.master_secret.clone(),
            cipher_suite,
            transcript_bytes,
            handshake_seq: 0,
            accumulated_plaintext: Vec::new(),
        };
        Ok(true)
    }

    fn process_encrypted_handshake(&mut self) -> io::Result<bool> {
        let State::ProcessingHandshake {
            client_hs_secret,
            server_hs_secret,
            master_secret,
            cipher_suite,
            transcript_bytes,
            handshake_seq,
            accumulated_plaintext,
        } = &self.state
        else {
            unreachable!()
        };

        // Clone everything we'll modify
        let client_hs_secret = client_hs_secret.clone();
        let server_hs_secret = server_hs_secret.clone();
        let master_secret = master_secret.clone();
        let cipher_suite = *cipher_suite;
        let transcript_bytes = transcript_bytes.clone();
        let mut handshake_seq = *handshake_seq;
        let mut accumulated_plaintext = accumulated_plaintext.clone();

        let (server_hs_key, server_hs_iv) = derive_traffic_keys(&server_hs_secret, cipher_suite)?;

        if self.ciphertext_read_buf.len() < TLS_RECORD_HEADER_SIZE {
            return Ok(false);
        }
        let record_type = self.ciphertext_read_buf[0];
        let record_len = self
            .ciphertext_read_buf
            .get_u16_be(3)
            .ok_or_else(|| io::Error::other("buffer too short"))? as usize;

        if record_len > 18432 {
            return Err(io::Error::other("TLS handshake record exceeds limit"));
        }
        if self.ciphertext_read_buf.len() < TLS_RECORD_HEADER_SIZE + record_len {
            return Ok(false);
        }
        // Compatibility records are complete, literal CCS messages only.
        if record_type == CONTENT_TYPE_CHANGE_CIPHER_SPEC {
            if self.ciphertext_read_buf[..TLS_RECORD_HEADER_SIZE + record_len]
                != [20, 3, 3, 0, 1, 1]
            {
                return Err(io::Error::other("invalid TLS ChangeCipherSpec"));
            }
            self.ciphertext_read_buf
                .consume(TLS_RECORD_HEADER_SIZE + record_len);
            return Ok(true);
        }
        if record_type != CONTENT_TYPE_APPLICATION_DATA {
            return Err(io::Error::other(format!(
                "expected app data record, got 0x{record_type:02x}"
            )));
        }

        let total = TLS_RECORD_HEADER_SIZE + record_len;
        if self.ciphertext_read_buf.len() < total {
            return Ok(false);
        }

        let ciphertext = self.ciphertext_read_buf[TLS_RECORD_HEADER_SIZE..total].to_vec();
        self.ciphertext_read_buf.consume(total);

        let plaintext = decrypt_handshake_message(
            cipher_suite,
            &server_hs_key,
            &server_hs_iv,
            handshake_seq,
            &ciphertext,
            record_len as u16,
        )?;
        handshake_seq += 1;

        if accumulated_plaintext.len().saturating_add(plaintext.len()) > 256 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "TLS handshake exceeds size limit",
            ));
        }
        accumulated_plaintext.extend_from_slice(&plaintext);

        // Re-scan from the beginning so a message split across TLS records is
        // parsed only after its complete body has arrived.
        let encrypted_extensions_offset =
            find_handshake_message(&accumulated_plaintext, HANDSHAKE_TYPE_ENCRYPTED_EXTENSIONS);
        let certificate_offset =
            find_handshake_message(&accumulated_plaintext, HANDSHAKE_TYPE_CERTIFICATE)
                .or_else(|| find_handshake_message(&accumulated_plaintext, 25));
        let cert_verify_offset =
            find_handshake_message(&accumulated_plaintext, HANDSHAKE_TYPE_CERTIFICATE_VERIFY);
        let finished_offset =
            find_handshake_message(&accumulated_plaintext, HANDSHAKE_TYPE_FINISHED);

        let (
            Some(extensions_offset),
            Some(certificate_offset),
            Some(cert_verify_offset),
            Some(finished_offset),
        ) = (
            encrypted_extensions_offset,
            certificate_offset,
            cert_verify_offset,
            finished_offset,
        )
        else {
            self.state = State::ProcessingHandshake {
                client_hs_secret,
                server_hs_secret,
                master_secret,
                cipher_suite,
                transcript_bytes,
                handshake_seq,
                accumulated_plaintext,
            };
            return Ok(true);
        };

        if extensions_offset != 0
            || extensions_offset >= certificate_offset
            || certificate_offset >= cert_verify_offset
            || cert_verify_offset >= finished_offset
            || finished_offset
                + 4
                + read_u24(&accumulated_plaintext[finished_offset + 1..finished_offset + 4])
                != accumulated_plaintext.len()
        {
            return Err(io::Error::other("invalid TLS server handshake ordering"));
        }
        self.negotiated_alpn = extensions::alpn(
            &accumulated_plaintext[extensions_offset..],
            &self.offered_alpn,
        )?;

        // Verify the certificate chain and bind the server identity to SNI.
        let certificate_len =
            read_u24(&accumulated_plaintext[certificate_offset + 1..certificate_offset + 4]);
        let certificate_message =
            &accumulated_plaintext[certificate_offset..certificate_offset + 4 + certificate_len];
        let certificate_message = crate::certificate::compression::decode(certificate_message)?;
        let certificates = parse_certificate_chain(&certificate_message)?;
        let verifier = match &self.config.server_verifier {
            Some(verifier) => Arc::clone(verifier),
            None => default_server_verifier()?,
        };
        verify_server_certificate(verifier.as_ref(), &certificates, &self.config.server_name)?;

        // Verify the TLS 1.3 CertificateVerify signature.
        let signature = parse_certificate_verify(&accumulated_plaintext[cert_verify_offset..])?;
        let mut cert_verify_transcript = digest::Context::new(cipher_suite.digest_algorithm());
        cert_verify_transcript.update(&transcript_bytes);
        cert_verify_transcript.update(&accumulated_plaintext[..cert_verify_offset]);
        let cert_verify_hash = cert_verify_transcript.finish();
        let cert_verify_message =
            tls13_server_certificate_verify_message(cert_verify_hash.as_ref());
        verify_server_signature(
            verifier.as_ref(),
            &cert_verify_message,
            &certificates[0],
            &signature,
        )?;

        verify_server_finished(
            cipher_suite,
            &server_hs_secret,
            &transcript_bytes,
            &accumulated_plaintext,
            finished_offset,
        )?;

        // Compute handshake hash and send client Finished
        let mut hs_ctx = digest::Context::new(cipher_suite.digest_algorithm());
        hs_ctx.update(&transcript_bytes);
        hs_ctx.update(&accumulated_plaintext);
        let hs_hash = hs_ctx.finish();

        let client_verify =
            compute_finished_verify_data(cipher_suite, &client_hs_secret, hs_hash.as_ref())?;
        let client_finished = construct_finished(&client_verify)?;

        let (client_hs_key, client_hs_iv) = derive_traffic_keys(&client_hs_secret, cipher_suite)?;
        let hs_aead = AeadKey::new(cipher_suite, &client_hs_key)?;
        let mut hs_seq = 0u64;
        let mut enc = RecordEncryptor::new(&hs_aead, &client_hs_iv, &mut hs_seq);
        enc.encrypt_handshake(&client_finished, &mut self.ciphertext_write_buf)?;

        // Derive application keys
        let (client_app_secret, server_app_secret) =
            derive_application_secrets(cipher_suite, &master_secret, hs_hash.as_ref())?;
        let (cak, caiv) = derive_traffic_keys(&client_app_secret, cipher_suite)?;
        let (sak, saiv) = derive_traffic_keys(&server_app_secret, cipher_suite)?;

        self.app_write_key = Some(AeadKey::new(cipher_suite, &cak)?);
        self.app_write_iv = Some(caiv);
        self.app_read_key = Some(AeadKey::new(cipher_suite, &sak)?);
        self.app_read_iv = Some(saiv);
        self.read_secret = Some(crate::post_handshake::traffic::TrafficSecret::new(
            cipher_suite,
            server_app_secret,
        ));
        self.write_secret = Some(crate::post_handshake::traffic::TrafficSecret::new(
            cipher_suite,
            client_app_secret,
        ));
        self.read_seq = 0;
        self.write_seq = 0;
        self.state = State::Complete;
        Ok(true)
    }
}

#[cfg(test)]
#[path = "../tests/unit/handshake_messages.rs"]
mod tests;

mod application;
mod key_update;

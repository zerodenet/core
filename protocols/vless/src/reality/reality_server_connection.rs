use std::io::{self, Read, Write};

use rcgen::{CertificateParams, KeyPair, SigningKey, PKCS_ED25519};
use ring::{digest, hmac};
use x25519_dalek::{PublicKey, StaticSecret};
use x509_parser::prelude::FromDer;

use ztls::aead::AeadKey;
use ztls::cipher::{CipherSuite, DEFAULT_CIPHER_SUITES};
use ztls::common::{
    ALERT_DESC_CLOSE_NOTIFY, ALERT_LEVEL_WARNING, CIPHERTEXT_READ_BUF_CAPACITY, CONTENT_TYPE_ALERT,
    CONTENT_TYPE_APPLICATION_DATA, CONTENT_TYPE_CHANGE_CIPHER_SPEC, CONTENT_TYPE_HANDSHAKE,
    HANDSHAKE_TYPE_FINISHED, OUTGOING_BUFFER_LIMIT, PLAINTEXT_READ_BUF_CAPACITY,
    TLS_MAX_RECORD_SIZE, TLS_RECORD_HEADER_SIZE,
};
use ztls::keys::{
    compute_finished_verify_data, derive_application_secrets, derive_handshake_keys,
    derive_traffic_keys,
};
use ztls::messages::{
    construct_certificate, construct_certificate_verify, construct_encrypted_extensions,
    construct_finished, construct_server_hello, write_record_header,
};
use ztls::reader_writer::{RealityReader, RealityWriter};
use ztls::reality_io_state::RealityIoState;
use ztls::record::{RecordDecryptor, RecordEncryptor};
use ztls::slide_buffer::SlideBuffer;
use ztls::util::{extract_client_cipher_suites, extract_session_id_slice, negotiate_cipher_suite};

#[derive(Clone)]
pub struct RealityServerConfig {
    pub policy: crate::reality_policy::ServerPolicy,
    pub mldsa65: Option<super::mldsa::Signing>,
    pub private_key: [u8; 32],
    pub short_ids: Vec<[u8; 8]>,
    pub server_name: String,
    pub cipher_suites: Vec<CipherSuite>,
    /// Handshake timeout in milliseconds (default: 10000 = 10 seconds)
    pub handshake_timeout_ms: u64,
}

impl Default for RealityServerConfig {
    fn default() -> Self {
        Self {
            policy: Default::default(),
            private_key: [0u8; 32],
            mldsa65: None,
            short_ids: Vec::new(),
            server_name: String::new(),
            cipher_suites: Vec::new(),
            handshake_timeout_ms: 10_000,
        }
    }
}

enum HandshakeState {
    AwaitingClientHello,
    AwaitingClientFinished {
        client_handshake_traffic_secret: Vec<u8>,
        master_secret: Vec<u8>,
        cipher_suite: CipherSuite,
        handshake_seq: u64,
        server_finished_hash: Vec<u8>,
        pending_finished: Vec<u8>,
    },
    Complete,
}

pub struct RealityServerConnection {
    tickets: ztls::post_handshake::TicketSink,
    read_secret: Option<ztls::post_handshake::traffic::TrafficSecret>,
    write_secret: Option<ztls::post_handshake::traffic::TrafficSecret>,
    target_shape: Option<super::target::Shape>,
    config: RealityServerConfig,
    hello: ztls::hello::ClientHelloMetadata,
    negotiated_alpn: Option<String>,
    handshake_state: HandshakeState,
    app_read_key: Option<AeadKey>,
    app_read_iv: Option<Vec<u8>>,
    app_write_key: Option<AeadKey>,
    app_write_iv: Option<Vec<u8>>,
    read_seq: u64,
    write_seq: u64,
    ccs_records: usize,
    tls_read_buffer: Box<[u8]>,
    ciphertext_read_buf: SlideBuffer,
    ciphertext_write_buf: Vec<u8>,
    plaintext_read_buf: SlideBuffer,
    plaintext_write_buf: Vec<u8>,
    received_close_notify: bool,
    fatal_error: Option<io::ErrorKind>,
    handshake_start: std::time::Instant,
}

impl RealityServerConnection {
    pub(crate) fn with_target_shape(mut self, shape: super::target::Shape) -> Self {
        self.target_shape = Some(shape);
        self
    }
    pub fn server_name(&self) -> Option<&str> {
        self.hello.server_name.as_deref()
    }
    pub fn negotiated_alpn(&self) -> Option<&str> {
        self.negotiated_alpn.as_deref()
    }
    pub fn new(config: RealityServerConfig) -> Self {
        Self {
            tickets: Default::default(),
            target_shape: None,
            config,
            hello: ztls::hello::ClientHelloMetadata::default(),
            negotiated_alpn: None,
            handshake_state: HandshakeState::AwaitingClientHello,
            read_secret: None,
            write_secret: None,
            app_read_key: None,
            app_read_iv: None,
            app_write_key: None,
            app_write_iv: None,
            read_seq: 0,
            write_seq: 0,
            ccs_records: 0,
            tls_read_buffer: vec![0_u8; TLS_MAX_RECORD_SIZE].into_boxed_slice(),
            ciphertext_read_buf: SlideBuffer::new(CIPHERTEXT_READ_BUF_CAPACITY),
            ciphertext_write_buf: Vec::with_capacity(OUTGOING_BUFFER_LIMIT),
            plaintext_read_buf: SlideBuffer::new(PLAINTEXT_READ_BUF_CAPACITY),
            plaintext_write_buf: Vec::with_capacity(OUTGOING_BUFFER_LIMIT),
            received_close_notify: false,
            fatal_error: None,
            handshake_start: std::time::Instant::now(),
        }
    }

    pub fn read_tls(&mut self, rd: &mut dyn Read) -> io::Result<usize> {
        if self.ciphertext_read_buf.remaining_capacity() < TLS_MAX_RECORD_SIZE {
            self.ciphertext_read_buf.compact();
        }

        let n = rd.read(&mut self.tls_read_buffer[..])?;
        if n > 0 {
            self.ciphertext_read_buf
                .extend_from_slice(&self.tls_read_buffer[..n]);
        }
        Ok(n)
    }

    pub(crate) fn next_tls_read_limit(&self) -> usize {
        if self.ciphertext_read_buf.len() < TLS_RECORD_HEADER_SIZE {
            return TLS_RECORD_HEADER_SIZE - self.ciphertext_read_buf.len();
        }

        let record_len =
            u16::from_be_bytes([self.ciphertext_read_buf[3], self.ciphertext_read_buf[4]]) as usize;
        (TLS_RECORD_HEADER_SIZE + record_len)
            .saturating_sub(self.ciphertext_read_buf.len())
            .max(1)
    }

    pub fn process_new_packets(&mut self) -> io::Result<RealityIoState> {
        if let Some(error_kind) = self.fatal_error {
            return Err(io::Error::new(error_kind, "connection previously failed"));
        }
        if self.received_close_notify {
            return Ok(RealityIoState::new(self.plaintext_read_buf.len()));
        }

        // Check for handshake timeout before processing
        if !matches!(self.handshake_state, HandshakeState::Complete) {
            let elapsed = self.handshake_start.elapsed();
            let timeout = std::time::Duration::from_millis(self.config.handshake_timeout_ms);
            if elapsed > timeout {
                self.fatal_error = Some(io::ErrorKind::TimedOut);
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!(
                        "Reality server handshake timed out after {}ms",
                        self.config.handshake_timeout_ms
                    ),
                ));
            }
        }

        let result = self.process_new_packets_inner();
        if let Err(ref error) = result {
            match error.kind() {
                io::ErrorKind::InvalidData
                | io::ErrorKind::PermissionDenied
                | io::ErrorKind::ConnectionAborted => {
                    self.fatal_error = Some(error.kind());
                }
                _ => {}
            }
        }
        result
    }

    fn process_new_packets_inner(&mut self) -> io::Result<RealityIoState> {
        loop {
            match self.handshake_state {
                HandshakeState::AwaitingClientHello => {
                    if !self.process_client_hello()? {
                        break;
                    }
                }
                HandshakeState::AwaitingClientFinished { .. } => {
                    if !self.process_client_finished()? {
                        break;
                    }
                }
                HandshakeState::Complete => {
                    self.process_application_data()?;
                    break;
                }
            }
        }

        Ok(RealityIoState::new(self.plaintext_read_buf.len()))
    }

    fn process_client_hello(&mut self) -> io::Result<bool> {
        let Some((record, consumed)) = super::hello::assembled(&self.ciphertext_read_buf[0..])?
        else {
            return Ok(false);
        };
        self.ciphertext_read_buf.consume(consumed);
        let client_hello = &record[TLS_RECORD_HEADER_SIZE..];
        self.hello = ztls::hello::client_hello_metadata(&record)?;
        let (auth_key, client_pk) = super::hello::authenticate(&self.config, &record)?;
        let encrypted_session_id = extract_session_id_slice(&record)?;
        let client_cipher_suites = extract_client_cipher_suites(&record)?;
        let server_preferences = if self.config.cipher_suites.is_empty() {
            DEFAULT_CIPHER_SUITES
        } else {
            &self.config.cipher_suites
        };
        let server_pref_ids: Vec<u16> = server_preferences.iter().map(|cs| cs.id()).collect();
        let cipher_suite = match &self.target_shape {
            Some(shape) => shape.suite()?,
            None => CipherSuite::from_id(negotiate_cipher_suite(
                &server_pref_ids,
                &client_cipher_suites,
            )?)
            .ok_or_else(|| io::Error::other("no common cipher suite"))?,
        };
        if !client_cipher_suites.contains(&cipher_suite.id())
            || !server_pref_ids.contains(&cipher_suite.id())
        {
            return Err(io::Error::other(
                "REALITY target selected an unavailable cipher",
            ));
        }
        let (server_hello, tls_shared_secret) = if let Some(shape) = &self.target_shape {
            shape.exchange(&record)?
        } else {
            let private = crate::mlkem::random::<32>();
            let public = PublicKey::from(&StaticSecret::from(private));
            let secret = super::reality_auth::perform_ecdh(&private, &client_pk)?;
            let hello = construct_server_hello(
                &crate::mlkem::random::<32>(),
                encrypted_session_id,
                cipher_suite.id(),
                public.as_bytes(),
            )?;
            (hello, secret.to_vec())
        };
        let mut server_hello_record =
            write_record_header(CONTENT_TYPE_HANDSHAKE, server_hello.len() as u16);
        server_hello_record.extend_from_slice(&server_hello);
        self.ciphertext_write_buf
            .extend_from_slice(&server_hello_record);

        let client_hello_hash_vec = {
            let mut ctx = digest::Context::new(cipher_suite.digest_algorithm());
            ctx.update(client_hello);
            ctx.finish().as_ref().to_vec()
        };
        let server_hello_hash_vec = {
            let mut ctx = digest::Context::new(cipher_suite.digest_algorithm());
            ctx.update(client_hello);
            ctx.update(&server_hello);
            ctx.finish().as_ref().to_vec()
        };
        let hs_keys = derive_handshake_keys(
            cipher_suite,
            &tls_shared_secret,
            &client_hello_hash_vec,
            &server_hello_hash_vec,
        )?;

        let mut transcript = Vec::new();
        transcript.extend_from_slice(client_hello);
        transcript.extend_from_slice(&server_hello);

        self.negotiated_alpn = ["h2", "http/1.1"]
            .into_iter()
            .find(|protocol| self.hello.alpn.iter().any(|offered| offered == protocol))
            .map(str::to_owned);
        let encrypted_extensions = construct_encrypted_extensions(self.negotiated_alpn.as_deref())?;
        let (certificate, cert_key) =
            construct_reality_certificate(&auth_key, self.config.mldsa65.as_ref(), &transcript)?;
        let certificate_message = construct_certificate(&certificate)?;

        let mut cv_transcript = digest::Context::new(cipher_suite.digest_algorithm());
        cv_transcript.update(&transcript);
        cv_transcript.update(&encrypted_extensions);
        cv_transcript.update(&certificate_message);
        let cv_hash = cv_transcript.finish();
        let certificate_verify_signature = sign_certificate_verify(&cert_key, cv_hash.as_ref())?;
        let certificate_verify = construct_certificate_verify(&certificate_verify_signature)?;

        let mut finished_transcript = digest::Context::new(cipher_suite.digest_algorithm());
        finished_transcript.update(&transcript);
        finished_transcript.update(&encrypted_extensions);
        finished_transcript.update(&certificate_message);
        finished_transcript.update(&certificate_verify);
        let finished_hash = finished_transcript.finish();
        let server_verify_data = compute_finished_verify_data(
            cipher_suite,
            &hs_keys.server_handshake_traffic_secret,
            finished_hash.as_ref(),
        )?;
        let server_finished = construct_finished(&server_verify_data)?;

        let mut handshake_plaintext = Vec::new();
        handshake_plaintext.extend_from_slice(&encrypted_extensions);
        handshake_plaintext.extend_from_slice(&certificate_message);
        handshake_plaintext.extend_from_slice(&certificate_verify);
        handshake_plaintext.extend_from_slice(&server_finished);

        let (server_hs_key, server_hs_iv) =
            derive_traffic_keys(&hs_keys.server_handshake_traffic_secret, cipher_suite)?;
        let server_hs_aead = AeadKey::new(cipher_suite, &server_hs_key)?;
        let mut server_hs_seq = 0;
        let mut encryptor =
            RecordEncryptor::new(&server_hs_aead, &server_hs_iv, &mut server_hs_seq);
        if let Some(shape) = &self.target_shape {
            shape.encrypt(
                &mut encryptor,
                [
                    &encrypted_extensions,
                    &certificate_message,
                    &certificate_verify,
                    &server_finished,
                ],
                &mut self.ciphertext_write_buf,
            )?;
        } else {
            encryptor.encrypt_handshake(&handshake_plaintext, &mut self.ciphertext_write_buf)?;
        }

        transcript.extend_from_slice(&handshake_plaintext);
        let server_finished_hash = {
            let mut ctx = digest::Context::new(cipher_suite.digest_algorithm());
            ctx.update(&transcript);
            ctx.finish().as_ref().to_vec()
        };

        self.handshake_state = HandshakeState::AwaitingClientFinished {
            client_handshake_traffic_secret: hs_keys.client_handshake_traffic_secret,
            master_secret: hs_keys.master_secret,
            cipher_suite,
            handshake_seq: 0,
            pending_finished: Vec::new(),
            server_finished_hash,
        };

        Ok(true)
    }

    fn process_client_finished(&mut self) -> io::Result<bool> {
        let HandshakeState::AwaitingClientFinished {
            client_handshake_traffic_secret,
            master_secret,
            cipher_suite,
            handshake_seq,
            server_finished_hash,
            pending_finished,
        } = &self.handshake_state
        else {
            unreachable!()
        };

        if self.ciphertext_read_buf.len() < TLS_RECORD_HEADER_SIZE {
            return Ok(false);
        }

        let record_type = self.ciphertext_read_buf[0];
        let record_len = self
            .ciphertext_read_buf
            .get_u16_be(3)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "buffer too short"))?
            as usize;
        let total_record_len = TLS_RECORD_HEADER_SIZE + record_len;
        if self.ciphertext_read_buf[1..3] != [3, 3] || record_len > 16384 + 256 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid encrypted TLS record header",
            ));
        }
        if self.ciphertext_read_buf.len() < total_record_len {
            return Ok(false);
        }

        if record_type == CONTENT_TYPE_CHANGE_CIPHER_SPEC {
            if record_len != 1 || self.ciphertext_read_buf[5] != 1 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid TLS ChangeCipherSpec",
                ));
            }
            self.ccs_records = self.ccs_records.saturating_add(1);
            if self.ccs_records
                > self
                    .target_shape
                    .as_ref()
                    .map_or(usize::MAX, super::target::Shape::max_ccs_records)
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "too many TLS ChangeCipherSpec records",
                ));
            }
            self.ciphertext_read_buf.consume(total_record_len);
            return Ok(true);
        }
        if record_type != CONTENT_TYPE_APPLICATION_DATA {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "expected encrypted Reality client Finished",
            ));
        }

        let client_hs_secret = client_handshake_traffic_secret.clone();
        let master_secret = master_secret.clone();
        let cipher_suite = *cipher_suite;
        let mut handshake_seq = *handshake_seq;
        let server_finished_hash = server_finished_hash.clone();
        let mut pending_finished = pending_finished.clone();

        let mut ciphertext =
            self.ciphertext_read_buf[TLS_RECORD_HEADER_SIZE..total_record_len].to_vec();
        self.ciphertext_read_buf.consume(total_record_len);

        let (client_hs_key, client_hs_iv) = derive_traffic_keys(&client_hs_secret, cipher_suite)?;
        let client_hs_aead = AeadKey::new(cipher_suite, &client_hs_key)?;
        let (content_type, plaintext) =
            RecordDecryptor::new(&client_hs_aead, &client_hs_iv, &mut handshake_seq)
                .decrypt_record_in_place(&mut ciphertext, record_len as u16)?;
        if content_type != CONTENT_TYPE_HANDSHAKE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "expected client Finished handshake content",
            ));
        }
        let expected_size = 4 + cipher_suite.digest_algorithm().output_len();
        if pending_finished.len().saturating_add(plaintext.len()) > expected_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "oversized client Finished",
            ));
        }
        pending_finished.extend_from_slice(plaintext);
        if pending_finished.len() < expected_size {
            self.handshake_state = HandshakeState::AwaitingClientFinished {
                client_handshake_traffic_secret: client_hs_secret,
                master_secret,
                cipher_suite,
                handshake_seq,
                server_finished_hash,
                pending_finished,
            };
            return Ok(true);
        }
        validate_client_finished(
            cipher_suite,
            &client_hs_secret,
            &server_finished_hash,
            &pending_finished,
        )?;

        let (client_app_secret, server_app_secret) =
            derive_application_secrets(cipher_suite, &master_secret, &server_finished_hash)?;
        let (client_app_key_bytes, client_app_iv) =
            derive_traffic_keys(&client_app_secret, cipher_suite)?;
        let (server_app_key_bytes, server_app_iv) =
            derive_traffic_keys(&server_app_secret, cipher_suite)?;

        self.app_read_key = Some(AeadKey::new(cipher_suite, &client_app_key_bytes)?);
        self.app_read_iv = Some(client_app_iv);
        self.app_write_key = Some(AeadKey::new(cipher_suite, &server_app_key_bytes)?);
        self.app_write_iv = Some(server_app_iv);
        self.read_secret = Some(ztls::post_handshake::traffic::TrafficSecret::new(
            cipher_suite,
            client_app_secret,
        ));
        self.write_secret = Some(ztls::post_handshake::traffic::TrafficSecret::new(
            cipher_suite,
            server_app_secret,
        ));
        self.read_seq = 0;
        self.write_seq = 0;
        if let (Some(shape), Some(key), Some(iv)) =
            (&self.target_shape, &self.app_write_key, &self.app_write_iv)
        {
            shape.encrypt_post_handshake(
                &mut RecordEncryptor::new(key, iv, &mut self.write_seq),
                &mut self.ciphertext_write_buf,
            )?;
        }
        self.handshake_state = HandshakeState::Complete;
        Ok(true)
    }

    fn process_application_data(&mut self) -> io::Result<()> {
        while self.ciphertext_read_buf.len() >= TLS_RECORD_HEADER_SIZE {
            let header = &self.ciphertext_read_buf;
            if header[0] != CONTENT_TYPE_APPLICATION_DATA
                || header[1..3] != [3, 3]
                || u16::from_be_bytes([header[3], header[4]]) as usize > 16384 + 256
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid encrypted TLS record header",
                ));
            }
            let (app_read_key, app_read_iv) = match (&self.app_read_key, &self.app_read_iv) {
                (Some(key), Some(iv)) => (key, iv),
                _ => unreachable!(),
            };

            let record_len = self
                .ciphertext_read_buf
                .get_u16_be(3)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "buffer too short"))?
                as usize;
            let total_record_len = TLS_RECORD_HEADER_SIZE + record_len;
            if self.ciphertext_read_buf.len() < total_record_len {
                break;
            }

            let ciphertext_slice = self
                .ciphertext_read_buf
                .slice_mut(TLS_RECORD_HEADER_SIZE..total_record_len);
            let (content_type, plaintext) =
                RecordDecryptor::new(app_read_key, app_read_iv, &mut self.read_seq)
                    .decrypt_record_in_place(ciphertext_slice, record_len as u16)?;

            let mut update = None;
            match content_type {
                CONTENT_TYPE_APPLICATION_DATA => {
                    self.tickets.application_data(!plaintext.is_empty())?;
                    self.plaintext_read_buf.maybe_compact(4096);
                    self.plaintext_read_buf.extend_from_slice(plaintext);
                }
                CONTENT_TYPE_ALERT => {
                    if plaintext.len() != 2 {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "invalid TLS alert length",
                        ));
                    }
                    if plaintext.len() == 2 {
                        let alert_level = plaintext[0];
                        let alert_desc = plaintext[1];
                        if alert_desc == ALERT_DESC_CLOSE_NOTIFY {
                            self.received_close_notify = true;
                            self.ciphertext_read_buf.consume(total_record_len);
                            return Ok(());
                        }
                        if alert_level != ALERT_LEVEL_WARNING {
                            return Err(io::Error::new(
                                io::ErrorKind::ConnectionAborted,
                                format!("received fatal alert: {alert_desc}"),
                            ));
                        }
                    }
                }
                CONTENT_TYPE_HANDSHAKE => update = self.tickets.receive_record(plaintext, false)?,
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "invalid or unsupported TLS post-handshake content type",
                    ))
                }
            }

            self.ciphertext_read_buf.consume(total_record_len);
            if let Some(requested) = update {
                self.receive_key_update(requested)?;
            }
        }

        Ok(())
    }

    pub fn reader(&mut self) -> RealityReader<'_> {
        self.plaintext_read_buf.maybe_compact(4096);
        RealityReader::new(&mut self.plaintext_read_buf, self.received_close_notify)
    }

    pub fn writer(&mut self) -> RealityWriter<'_> {
        RealityWriter::new(&mut self.plaintext_write_buf)
    }

    pub fn write_tls(&mut self, wr: &mut dyn Write) -> io::Result<usize> {
        if !matches!(self.handshake_state, HandshakeState::Complete) {
            let n = wr.write(&self.ciphertext_write_buf)?;
            self.ciphertext_write_buf.drain(..n);
            return Ok(n);
        }

        if !self.plaintext_write_buf.is_empty() {
            let (app_write_key, app_write_iv) = match (&self.app_write_key, &self.app_write_iv) {
                (Some(key), Some(iv)) => (key, iv),
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "application keys not available",
                    ));
                }
            };
            RecordEncryptor::new(app_write_key, app_write_iv, &mut self.write_seq)
                .encrypt_app_data(
                    &mut self.plaintext_write_buf,
                    &mut self.ciphertext_write_buf,
                )?;
        }

        let n = wr.write(&self.ciphertext_write_buf)?;
        self.ciphertext_write_buf.drain(..n);
        Ok(n)
    }

    pub fn wants_write(&self) -> bool {
        !self.ciphertext_write_buf.is_empty() || !self.plaintext_write_buf.is_empty()
    }

    pub fn wants_read(&self) -> bool {
        if self.received_close_notify || self.fatal_error.is_some() {
            return false;
        }
        self.is_handshaking() || self.plaintext_read_buf.is_empty()
    }

    pub fn is_handshaking(&self) -> bool {
        !matches!(self.handshake_state, HandshakeState::Complete)
    }

    pub fn send_close_notify(&mut self) {
        if !matches!(self.handshake_state, HandshakeState::Complete) {
            return;
        }
        if let (Some(key), Some(iv)) = (&self.app_write_key, &self.app_write_iv) {
            let _ = RecordEncryptor::new(key, iv, &mut self.write_seq)
                .encrypt_close_notify(&mut self.ciphertext_write_buf);
        }
    }
}

fn construct_reality_certificate(
    auth_key: &[u8; 32],
    mldsa: Option<&super::mldsa::Signing>,
    hellos: &[u8],
) -> io::Result<(Vec<u8>, std::sync::Arc<KeyPair>)> {
    static SIGNER: std::sync::LazyLock<Result<std::sync::Arc<KeyPair>, String>> =
        std::sync::LazyLock::new(|| {
            KeyPair::generate_for(&PKCS_ED25519)
                .map(std::sync::Arc::new)
                .map_err(|error| error.to_string())
        });
    let key_pair = SIGNER
        .as_ref()
        .map_err(|error| io::Error::other(error.clone()))?
        .clone();
    let mut params = CertificateParams::default();
    params.distinguished_name = rcgen::DistinguishedName::new();
    params.serial_number = Some(0u64.into());
    params.not_before = rcgen::date_time_ymd(1, 1, 1);
    params.not_after = params.not_before;
    if let Some(key) = mldsa {
        params
            .custom_extensions
            .push(rcgen::CustomExtension::from_oid_content(
                &[0, 0],
                super::mldsa::sign(key, auth_key, key_pair.public_key_raw(), hellos)?,
            ));
    }
    let cert = params
        .self_signed(key_pair.as_ref())
        .map_err(io::Error::other)?;
    let mut cert_der = cert.der().to_vec();
    let (_, parsed) =
        x509_parser::prelude::X509Certificate::from_der(&cert_der).map_err(io::Error::other)?;
    let signature = parsed.signature_value.data.as_ref();
    let offset = signature.as_ptr() as usize - cert_der.as_ptr() as usize;
    let tag = hmac::sign(
        &hmac::Key::new(hmac::HMAC_SHA512, auth_key),
        key_pair.public_key_raw(),
    );
    cert_der[offset..offset + 64].copy_from_slice(tag.as_ref());
    Ok((cert_der, key_pair))
}

fn sign_certificate_verify(key_pair: &KeyPair, transcript_hash: &[u8]) -> io::Result<Vec<u8>> {
    let mut signed_content = Vec::with_capacity(64 + 34 + transcript_hash.len());
    signed_content.extend_from_slice(&[0x20u8; 64]);
    signed_content.extend_from_slice(b"TLS 1.3, server CertificateVerify");
    signed_content.push(0x00);
    signed_content.extend_from_slice(transcript_hash);
    key_pair
        .sign(&signed_content)
        .map_err(|error| io::Error::other(error.to_string()))
}

fn validate_client_finished(
    cipher_suite: CipherSuite,
    client_hs_secret: &[u8],
    handshake_hash: &[u8],
    plaintext: &[u8],
) -> io::Result<()> {
    if plaintext.len() < 4 || plaintext[0] != HANDSHAKE_TYPE_FINISHED {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "expected client Finished message",
        ));
    }
    let len = u32::from_be_bytes([0, plaintext[1], plaintext[2], plaintext[3]]) as usize;
    if plaintext.len() != 4 + len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "client Finished message length mismatch",
        ));
    }
    let expected = compute_finished_verify_data(cipher_suite, client_hs_secret, handshake_hash)?;
    if plaintext[4..] != expected {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "client Finished verification failed",
        ));
    }
    Ok(())
}

#[cfg(test)]
#[path = "../../tests/reality/certificate.rs"]
mod certificate_tests;

#[path = "reality_server_connection/key_update.rs"]
mod key_update;

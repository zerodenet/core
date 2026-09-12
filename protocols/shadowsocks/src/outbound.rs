// Shadowsocks outbound protocol.

use zero_core::ProtocolType;
#[cfg(feature = "crypto")]
use zero_core::{Error, Session};
#[cfg(feature = "crypto")]
use zero_traits::{AsyncSocket, TcpSessionProtocol};

/// Shadowsocks outbound handler.
#[derive(Debug, Default, Clone, Copy)]
pub struct ShadowsocksOutbound;

#[cfg(feature = "crypto")]
pub struct ShadowsocksOutboundSession {
    pub legacy: Option<crate::shared::legacy::LegacyCipherState>,
    pub session_key: Vec<u8>,
    pub next_upload_nonce: u128,
    pub cipher: super::shared::CipherKind,
    /// For 2022 edition: the request salt we sent. The stream verifies the
    /// server's response header echoes this salt. Empty for legacy AEAD.
    pub request_salt: Vec<u8>,
}

impl ShadowsocksOutbound {
    pub fn protocol(&self) -> ProtocolType {
        ProtocolType::new("shadowsocks")
    }

    /// Write the initial encrypted stream payload containing target address.
    #[cfg(feature = "crypto")]
    pub async fn send_request<S: AsyncSocket>(
        &self,
        stream: &mut S,
        session: &Session,
        cipher: super::shared::CipherKind,
        password: &[u8],
    ) -> Result<ShadowsocksOutboundSession, Error> {
        if cipher.is_stream() {
            return crate::shared::legacy::send_request(stream, session, cipher, password, None)
                .await;
        }
        if cipher.is_blake3() {
            #[cfg(feature = "blake3")]
            {
                return self
                    .send_request_2022(stream, session, cipher, password)
                    .await;
            }
            #[cfg(not(feature = "blake3"))]
            return Err(Error::Protocol(
                "ss: 2022 tcp request requires `blake3` feature",
            ));
        }
        self.send_request_legacy(stream, session, cipher, password, None)
            .await
    }

    /// Legacy AEAD request: salt + one length/payload chunk carrying the
    /// target address (the first body chunk).
    #[cfg(feature = "crypto")]
    pub(super) async fn send_request_legacy<S: AsyncSocket>(
        &self,
        stream: &mut S,
        session: &Session,
        cipher: super::shared::CipherKind,
        password: &[u8],
        replay: Option<&crate::shared::legacy_replay::LegacyReplay>,
    ) -> Result<ShadowsocksOutboundSession, Error> {
        use super::shared::{build_target_data, derive_session_key, encrypt_tcp_chunk};

        let salt_len = cipher.salt_len();

        let salt = match replay {
            Some(replay) => replay.generate(salt_len)?,
            None => {
                let mut salt = vec![0; salt_len];
                super::shared::fill_random(&mut salt)?;
                salt
            }
        };

        let key = derive_session_key(cipher, password, &salt)?;

        // Build target data
        let target_data = build_target_data(&session.target, session.port, &[])?;

        let mut nonce = 0;
        let encrypted = encrypt_tcp_chunk(cipher, &key, &mut nonce, &target_data)?;

        let mut request = Vec::with_capacity(salt.len() + encrypted.len());
        request.extend_from_slice(&salt);
        request.extend_from_slice(&encrypted);
        stream
            .write_all(&request)
            .await
            .map_err(|_| Error::Io("ss: failed to write request"))?;

        Ok(ShadowsocksOutboundSession {
            legacy: None,
            session_key: key,
            next_upload_nonce: nonce,
            cipher,
            request_salt: salt,
        })
    }

    /// 2022 edition (SIP022) request: salt + fixed-header chunk (nonce 0) +
    /// variable-header chunk (nonce 1). Body chunks follow from nonce 2.
    #[cfg(all(feature = "crypto", feature = "blake3"))]
    async fn send_request_2022<S: AsyncSocket>(
        &self,
        stream: &mut S,
        session: &Session,
        cipher: super::shared::CipherKind,
        password: &[u8],
    ) -> Result<ShadowsocksOutboundSession, Error> {
        use super::shared::{
            build_2022_request_fixed_header, build_2022_request_var_header, derive_session_key,
            encode_tcp_2022_identity_headers, encrypt_tcp_2022_single_chunk, now_unix_seconds,
            parse_2022_key_chain, random_2022_padding,
        };

        let key_chain = parse_2022_key_chain(cipher, password)?;

        let salt_len = cipher.salt_len();
        let mut salt = vec![0u8; salt_len];
        use ring::rand::SecureRandom;
        ring::rand::SystemRandom::new()
            .fill(&mut salt)
            .map_err(|_| Error::Protocol("ss: random failed"))?;

        let key = derive_session_key(cipher, &key_chain.user_password, &salt)?;
        let identity_headers = encode_tcp_2022_identity_headers(
            cipher,
            &key_chain.identity_keys,
            &key_chain.user_key,
            &salt,
        )?;

        // No initial payload is available at request time (the relay has not
        // started), so per SIP022 3.1.3 we MUST include non-zero padding.
        let padding = random_2022_padding(true)?;
        let var_header =
            build_2022_request_var_header(&session.target, session.port, &padding, &[])?;
        let var_len = u16::try_from(var_header.len())
            .map_err(|_| Error::Protocol("ss: 2022 variable header too long"))?;

        let fixed_header = build_2022_request_fixed_header(now_unix_seconds(), var_len);

        let mut nonce = 0u128;
        let enc_fixed = encrypt_tcp_2022_single_chunk(cipher, &key, &mut nonce, &fixed_header)?;
        let enc_var = encrypt_tcp_2022_single_chunk(cipher, &key, &mut nonce, &var_header)?;

        let mut request = Vec::with_capacity(
            salt.len() + identity_headers.len() + enc_fixed.len() + enc_var.len(),
        );
        request.extend_from_slice(&salt);
        request.extend_from_slice(&identity_headers);
        request.extend_from_slice(&enc_fixed);
        request.extend_from_slice(&enc_var);
        stream
            .write_all(&request)
            .await
            .map_err(|_| Error::Io("ss: failed to write 2022 request"))?;

        Ok(ShadowsocksOutboundSession {
            legacy: None,
            session_key: key,
            // Body length+payload chunks start at nonce 2 (after the two
            // header chunks consumed nonces 0 and 1).
            next_upload_nonce: nonce,
            cipher,
            request_salt: salt,
        })
    }
}

/// Target parameters for Shadowsocks TCP session.
mod tcp;
pub use tcp::{tcp_connect_config_from_config, ShadowsocksTcpConnectConfig, ShadowsocksTcpTarget};

use super::*;
impl ShadowsocksInbound {
    pub fn protocol(&self) -> ProtocolType {
        ProtocolType::new("shadowsocks")
    }

    /// Decrypt the initial stream payload, extract target address,
    /// and return session key + remaining payload for relay.
    #[cfg(feature = "crypto")]
    pub async fn accept_request<S: zero_traits::AsyncSocket>(
        &self,
        stream: &mut S,
        cipher: crate::shared::CipherKind,
        password: &[u8],
    ) -> Result<ShadowsocksAccept, Error> {
        if cipher.is_stream() {
            return crate::shared::legacy::accept_request(stream, cipher, password).await;
        }
        if cipher.is_blake3() {
            #[cfg(feature = "blake3")]
            {
                return self.accept_request_2022(stream, cipher, password).await;
            }
            #[cfg(not(feature = "blake3"))]
            return Err(Error::Protocol(
                "ss: 2022 tcp accept requires `blake3` feature",
            ));
        }
        self.accept_request_legacy(stream, cipher, password).await
    }

    #[cfg(feature = "crypto")]
    pub(super) async fn accept_request_users<S: zero_traits::AsyncSocket>(
        &self,
        stream: &mut S,
        cipher: crate::shared::CipherKind,
        identity_password: Option<&[u8]>,
        users: &ShadowsocksAuthorizedUsers,
    ) -> Result<(ShadowsocksAccept, usize), Error> {
        if users.is_empty() {
            return Err(Error::Protocol("ss: no authorized inbound users"));
        }
        if cipher.is_blake3() {
            let Some(_identity_password) = identity_password else {
                if users.len() > 1 {
                    return Err(Error::Protocol(
                        "ss: 2022 multi-user tcp accept requires a SIP023 identity key",
                    ));
                }
                return self
                    .accept_request(stream, cipher, users[0].password())
                    .await
                    .map(|accept| (accept, 0));
            };
            #[cfg(feature = "blake3")]
            {
                return self
                    .accept_request_2022_eih(stream, cipher, _identity_password, users)
                    .await;
            }
            #[cfg(not(feature = "blake3"))]
            return Err(Error::Protocol(
                "ss: SIP023 tcp accept requires `blake3` feature",
            ));
        }
        if users.len() == 1 {
            return self
                .accept_request(stream, cipher, users[0].password())
                .await
                .map(|accept| (accept, 0));
        }
        if cipher.is_stream() {
            return Err(Error::Protocol(
                "ss: stream methods require one user per listener",
            ));
        }
        self.accept_request_legacy_users(stream, cipher, users)
            .await
    }

    /// Encrypt a plaintext chunk for the server-to-client direction.
    #[cfg(feature = "crypto")]
    pub fn encrypt_chunk(
        cipher: crate::shared::CipherKind,
        key: &[u8],
        nonce_counter: &mut u128,
        data: &[u8],
    ) -> Result<Vec<u8>, Error> {
        crate::shared::encrypt_tcp_chunk(cipher, key, nonce_counter, data)
    }

    /// Decrypt a ciphertext chunk for the client-to-server direction.
    #[cfg(feature = "crypto")]
    pub fn decrypt_chunk(
        cipher: crate::shared::CipherKind,
        key: &[u8],
        nonce_counter: &mut u128,
        data: &[u8],
    ) -> Result<Vec<u8>, Error> {
        let length_size = crate::shared::TCP_CHUNK_SIZE_LEN + cipher.tag_len();
        if data.len() < length_size {
            return Err(Error::Protocol("ss: chunk too short"));
        }
        let payload_len = crate::shared::decrypt_tcp_chunk_length(
            cipher,
            key,
            nonce_counter,
            &data[..length_size],
        )?;
        crate::shared::decrypt_tcp_chunk_payload(
            cipher,
            key,
            nonce_counter,
            payload_len,
            &data[length_size..],
        )
    }
}

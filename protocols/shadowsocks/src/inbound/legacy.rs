use super::*;
impl ShadowsocksInbound {
    #[cfg(feature = "crypto")]
    pub(super) async fn accept_request_legacy_users<S: zero_traits::AsyncSocket>(
        &self,
        stream: &mut S,
        cipher: crate::shared::CipherKind,
        users: &ShadowsocksAuthorizedUsers,
    ) -> Result<(ShadowsocksAccept, usize), Error> {
        use crate::shared::{
            decrypt_tcp_chunk_length, decrypt_tcp_chunk_payload, derive_session_key,
            parse_target_data, read_exact, TCP_CHUNK_SIZE_LEN,
        };

        let salt_len = cipher.salt_len();
        let mut salt = vec![0u8; salt_len];
        read_exact(stream, &mut salt).await?;
        let mut encrypted_length = vec![0u8; TCP_CHUNK_SIZE_LEN + cipher.tag_len()];
        read_exact(stream, &mut encrypted_length).await?;

        let mut matched = None;
        for (index, user) in users.iter().enumerate() {
            let key = derive_session_key(cipher, user.password(), &salt)?;
            let mut nonce = 0;
            if let Ok(payload_len) =
                decrypt_tcp_chunk_length(cipher, &key, &mut nonce, &encrypted_length)
            {
                matched = Some((index, key, nonce, payload_len));
                break;
            }
        }
        let Some((user_index, key, mut nonce, payload_len)) = matched else {
            return Err(Error::Protocol("ss: inbound user authentication failed"));
        };

        let mut encrypted_payload = vec![0u8; payload_len + cipher.tag_len()];
        read_exact(stream, &mut encrypted_payload).await?;
        let plain =
            decrypt_tcp_chunk_payload(cipher, &key, &mut nonce, payload_len, &encrypted_payload)?;
        let plain =
            crate::shared::complete_tcp_target(stream, cipher, &key, &mut nonce, plain).await?;
        let (target, port, payload_offset) = parse_target_data(&plain)?;
        let session = Session::new(
            0,
            target,
            port,
            Network::Tcp,
            ProtocolType::new("shadowsocks"),
        );
        Ok((
            ShadowsocksAccept {
                legacy: None,
                session,
                remaining_payload: plain[payload_offset..].to_vec(),
                session_key: key,
                cipher,
                next_upload_nonce: nonce,
                request_salt: salt,
            },
            user_index,
        ))
    }

    /// Legacy AEAD accept: read salt + one length/payload chunk, extract target.
    #[cfg(feature = "crypto")]
    pub(super) async fn accept_request_legacy<S: zero_traits::AsyncSocket>(
        &self,
        stream: &mut S,
        cipher: crate::shared::CipherKind,
        password: &[u8],
    ) -> Result<ShadowsocksAccept, Error> {
        use crate::shared::{derive_session_key, parse_target_data, read_exact, read_tcp_chunk};

        let salt_len = cipher.salt_len();

        // Read salt
        let mut salt = vec![0u8; salt_len];
        read_exact(stream, &mut salt).await?;

        let key = derive_session_key(cipher, password, &salt)?;

        let mut nonce = 0;
        let plain = read_tcp_chunk(stream, cipher, &key, &mut nonce).await?;

        let plain =
            crate::shared::complete_tcp_target(stream, cipher, &key, &mut nonce, plain).await?;
        let (target, port, payload_offset) = parse_target_data(&plain)?;
        let remaining_payload = plain[payload_offset..].to_vec();

        let session = Session::new(
            0,
            target,
            port,
            Network::Tcp,
            ProtocolType::new("shadowsocks"),
        );

        Ok(ShadowsocksAccept {
            legacy: None,
            session,
            remaining_payload,
            session_key: key,
            cipher,
            next_upload_nonce: nonce,
            request_salt: salt,
        })
    }
}

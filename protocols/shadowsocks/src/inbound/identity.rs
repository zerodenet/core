use super::*;
impl ShadowsocksInbound {
    #[cfg(all(feature = "crypto", feature = "blake3"))]
    pub(super) async fn accept_request_2022_eih<S: zero_traits::AsyncSocket>(
        &self,
        stream: &mut S,
        cipher: crate::shared::CipherKind,
        identity_password: &[u8],
        users: &ShadowsocksAuthorizedUsers,
    ) -> Result<(ShadowsocksAccept, usize), Error> {
        match self
            .accept_request_2022_eih_probe(stream, cipher, identity_password, users)
            .await
        {
            Ok(accept) => Ok(accept),
            Err(error) => {
                drain_stream(stream, SS_2022_DRAIN_CAP).await;
                Err(error)
            }
        }
    }

    #[cfg(all(feature = "crypto", feature = "blake3"))]
    pub(super) async fn accept_request_2022_eih_probe<S: zero_traits::AsyncSocket>(
        &self,
        stream: &mut S,
        cipher: crate::shared::CipherKind,
        identity_password: &[u8],
        users: &ShadowsocksAuthorizedUsers,
    ) -> Result<(ShadowsocksAccept, usize), Error> {
        use crate::shared::{
            decrypt_tcp_2022_identity_header, decrypt_tcp_2022_single_chunk, derive_session_key,
            parse_2022_request_fixed_header, parse_2022_request_var_header,
            validate_2022_timestamp, SS_2022_HEADER_TYPE_CLIENT_STREAM,
            SS_2022_REQUEST_FIXED_HEADER_LEN,
        };

        let salt_len = cipher.salt_len();
        let fixed_size = SS_2022_REQUEST_FIXED_HEADER_LEN + cipher.tag_len();
        let mut head = vec![0u8; salt_len + 16 + fixed_size];
        let n = stream
            .read(&mut head)
            .await
            .map_err(|_| Error::Io("ss: 2022 request read failed"))?;
        if n < head.len() {
            return Err(Error::Protocol("ss: 2022 request header too short"));
        }

        let identity = decrypt_tcp_2022_identity_header(
            cipher,
            identity_password,
            &head[..salt_len],
            &head[salt_len..salt_len + 16],
        )?;
        let (user_index, user) = users
            .find_identity(&identity)
            .ok_or(Error::Protocol("ss: SIP023 tcp user identity not found"))?;
        let key = derive_session_key(cipher, user.password(), &head[..salt_len])?;
        let mut nonce = 0u128;
        let fixed_plain = decrypt_tcp_2022_single_chunk(
            cipher,
            &key,
            &mut nonce,
            &head[salt_len + 16..salt_len + 16 + fixed_size],
        )?;
        let (header_type, timestamp, var_len) = parse_2022_request_fixed_header(&fixed_plain)?;
        if header_type != SS_2022_HEADER_TYPE_CLIENT_STREAM {
            return Err(Error::Protocol("ss: SIP023 request header bad type"));
        }
        validate_2022_timestamp(timestamp)?;
        let var_len = var_len as usize;

        let var_size = var_len + cipher.tag_len();
        let mut enc_var = vec![0u8; var_size];
        crate::shared::read_exact(stream, &mut enc_var).await?;
        let var_plain =
            decrypt_tcp_2022_single_chunk(cipher, &key, &mut nonce, &enc_var[..var_size])?;
        let (target, port, initial_payload) = parse_2022_request_var_header(&var_plain)?;
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
                remaining_payload: initial_payload,
                session_key: key,
                cipher,
                next_upload_nonce: nonce,
                request_salt: head[..salt_len].to_vec(),
            },
            user_index,
        ))
    }
}

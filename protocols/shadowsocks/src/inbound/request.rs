use super::*;
impl ShadowsocksInbound {
    /// 2022 edition (SIP022) accept: read salt + fixed-header chunk (nonce 0)
    /// + variable-header chunk (nonce 1). Body chunks follow from nonce 2.
    ///
    /// Implements SIP022 3.1.3 detection prevention: the salt + fixed-length
    /// header are read in a single `read()` call, and on any handshake failure
    /// the stream is drained before returning so the subsequent close sends FIN
    /// rather than RST (hiding how many bytes the server consumed).
    #[cfg(all(feature = "crypto", feature = "blake3"))]
    pub(super) async fn accept_request_2022<S: zero_traits::AsyncSocket>(
        &self,
        stream: &mut S,
        cipher: crate::shared::CipherKind,
        password: &[u8],
    ) -> Result<ShadowsocksAccept, Error> {
        match self
            .accept_request_2022_probe(stream, cipher, password)
            .await
        {
            Ok(accept) => Ok(accept),
            Err(error) => {
                // Drain to hide byte consumption from active probers.
                drain_stream(stream, SS_2022_DRAIN_CAP).await;
                Err(error)
            }
        }
    }

    /// Single-read + validate the 2022 request, without drain-on-error. The
    /// caller ([`accept_request_2022`]) drains on failure.
    #[cfg(all(feature = "crypto", feature = "blake3"))]
    pub(super) async fn accept_request_2022_probe<S: zero_traits::AsyncSocket>(
        &self,
        stream: &mut S,
        cipher: crate::shared::CipherKind,
        password: &[u8],
    ) -> Result<ShadowsocksAccept, Error> {
        use crate::shared::{
            decrypt_tcp_2022_single_chunk, derive_session_key, parse_2022_request_fixed_header,
            parse_2022_request_var_header, validate_2022_timestamp,
            SS_2022_HEADER_TYPE_CLIENT_STREAM, SS_2022_REQUEST_FIXED_HEADER_LEN,
        };

        let salt_len = cipher.salt_len();
        let fixed_size = SS_2022_REQUEST_FIXED_HEADER_LEN + cipher.tag_len();

        // SIP022 3.1.3: exactly ONE read for salt + fixed-length header. A
        // short read means a probe (or a fragmenting path); reject it.
        let mut head = vec![0u8; salt_len + fixed_size];
        let n = stream
            .read(&mut head)
            .await
            .map_err(|_| Error::Io("ss: 2022 request read failed"))?;
        if n < salt_len + fixed_size {
            return Err(Error::Protocol("ss: 2022 request header too short"));
        }

        let key = derive_session_key(cipher, password, &head[..salt_len])?;
        let mut nonce = 0u128;
        let fixed_plain = decrypt_tcp_2022_single_chunk(
            cipher,
            &key,
            &mut nonce,
            &head[salt_len..salt_len + fixed_size],
        )?;
        let (header_type, timestamp, var_len) = parse_2022_request_fixed_header(&fixed_plain)?;
        if header_type != SS_2022_HEADER_TYPE_CLIENT_STREAM {
            return Err(Error::Protocol("ss: 2022 request header bad type"));
        }
        validate_2022_timestamp(timestamp)?;

        // Only salt + fixed header require a single read. Continue the variable
        // data chunk across ordinary stream fragmentation.
        let var_len = var_len as usize;
        let var_size = var_len + cipher.tag_len();
        let mut enc_var = vec![0u8; var_size];
        crate::shared::read_exact(stream, &mut enc_var).await?;
        let var_plain =
            decrypt_tcp_2022_single_chunk(cipher, &key, &mut nonce, &enc_var[..var_size])?;
        if var_plain.len() != var_len {
            return Err(Error::Protocol("ss: 2022 variable header length mismatch"));
        }
        let (target, port, initial_payload) = parse_2022_request_var_header(&var_plain)?;

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
            remaining_payload: initial_payload,
            session_key: key,
            cipher,
            next_upload_nonce: nonce,
            request_salt: head[..salt_len].to_vec(),
        })
    }
}

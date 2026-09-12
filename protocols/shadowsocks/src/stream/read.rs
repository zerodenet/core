use super::*;
impl<S> ShadowsocksAeadStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn serve_read_plain(&mut self, buf: &mut ReadBuf<'_>) -> bool {
        if self.read_plain_pos >= self.read_plain.len() {
            self.read_plain.clear();
            self.read_plain_pos = 0;
            return false;
        }

        let available = &self.read_plain[self.read_plain_pos..];
        let n = available.len().min(buf.remaining());
        buf.put_slice(&available[..n]);
        self.read_plain_pos += n;
        true
    }

    pub(super) fn poll_read_decrypted(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 || self.serve_read_plain(buf) {
            return Poll::Ready(Ok(()));
        }

        loop {
            match &mut self.read_state {
                ReadState::Legacy => {
                    let mut plain = vec![0; buf.remaining().min(65535)];
                    let mut encrypted = ReadBuf::new(&mut plain);
                    match Pin::new(&mut self.inner).poll_read(cx, &mut encrypted) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(result) => result?,
                    }
                    let size = encrypted.filled().len();
                    if let Some(state) = &mut self.legacy_read {
                        state.decrypt(&mut plain[..size]);
                    }
                    buf.put_slice(&plain[..size]);
                    return Poll::Ready(Ok(()));
                }

                ReadState::Salt { buf: salt, pos } => {
                    match poll_fill(&mut self.inner, cx, salt, pos, false)? {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(()) => {
                            let password = self.read_password.take().ok_or_else(|| {
                                io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    "shadowsocks read password missing",
                                )
                            })?;
                            if self.cipher.is_stream() {
                                self.replay.check(salt).map_err(|error| {
                                    io::Error::new(io::ErrorKind::InvalidData, error)
                                })?;
                                self.legacy_read = Some(
                                    crate::shared::legacy::LegacyCipherState::new(
                                        self.cipher,
                                        &password,
                                        salt,
                                    )
                                    .map_err(|error| {
                                        io::Error::new(io::ErrorKind::InvalidData, error)
                                    })?,
                                );
                                self.read_state = ReadState::Legacy;
                                continue;
                            }
                            self.read_key =
                                Some(derive_download_key(self.cipher, &password, salt).map_err(
                                    |error| io::Error::new(io::ErrorKind::InvalidData, error),
                                )?);
                            if self.is_2022 {
                                self.modern_response_salt = Some(salt.clone());
                                // 2022 response: read the fixed-length header
                                // chunk next (carries request salt + first
                                // payload length).
                                let header_len =
                                    crate::shared::ss_2022_response_header_plain_len(salt.len())
                                        + self.cipher.tag_len();
                                self.read_state = ReadState::ResponseHeader2022 {
                                    buf: vec![0_u8; header_len],
                                    pos: 0,
                                };
                            } else {
                                self.legacy_response_salt = Some(salt.clone());
                                self.read_state = ReadState::Length {
                                    buf: vec![0_u8; TCP_CHUNK_SIZE_LEN + self.cipher.tag_len()],
                                    pos: 0,
                                };
                            }
                        }
                    }
                }
                ReadState::ResponseHeader2022 { buf, pos } => {
                    match poll_fill(&mut self.inner, cx, buf, pos, false)? {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(()) => {
                            let key = self.read_key.as_ref().ok_or_else(|| {
                                io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    "shadowsocks read key missing",
                                )
                            })?;
                            let header_plain = crate::shared::decrypt_tcp_2022_single_chunk(
                                self.cipher,
                                key,
                                &mut self.read_nonce,
                                buf,
                            )
                            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
                            let salt_len = self.cipher.salt_len();
                            let (header_type, _timestamp, resp_request_salt, length) =
                                crate::shared::parse_2022_response_fixed_header(
                                    &header_plain,
                                    salt_len,
                                )
                                .map_err(|error| {
                                    io::Error::new(io::ErrorKind::InvalidData, error)
                                })?;
                            if header_type != crate::shared::SS_2022_HEADER_TYPE_SERVER_STREAM {
                                return Poll::Ready(Err(io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    "ss: 2022 response header bad type",
                                )));
                            }
                            #[cfg(feature = "blake3")]
                            {
                                crate::shared::validate_2022_timestamp(_timestamp).map_err(
                                    |error| io::Error::new(io::ErrorKind::InvalidData, error),
                                )?;
                            }
                            // SIP022 3.1.3: the client MUST verify the echoed request salt.
                            if resp_request_salt != self.request_salt {
                                return Poll::Ready(Err(io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    "ss: 2022 response request salt mismatch",
                                )));
                            }
                            #[cfg(feature = "blake3")]
                            if let Some(salt) = self.modern_response_salt.take() {
                                self.replay.check_2022(&salt).map_err(|error| {
                                    io::Error::new(io::ErrorKind::InvalidData, error)
                                })?;
                            }
                            self.read_state = ReadState::FirstPayload2022 {
                                expected_len: length as usize,
                                buf: vec![0_u8; length as usize + self.cipher.tag_len()],
                                pos: 0,
                            };
                        }
                    }
                }
                ReadState::FirstPayload2022 {
                    expected_len,
                    buf: encrypted,
                    pos,
                } => match poll_fill(&mut self.inner, cx, encrypted, pos, false)? {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(()) => {
                        let key = self.read_key.as_ref().ok_or_else(|| {
                            io::Error::new(
                                io::ErrorKind::InvalidData,
                                "shadowsocks read key missing",
                            )
                        })?;
                        self.read_plain = crate::shared::decrypt_tcp_2022_single_chunk(
                            self.cipher,
                            key,
                            &mut self.read_nonce,
                            encrypted,
                        )
                        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
                        if self.read_plain.len() != *expected_len {
                            return Poll::Ready(Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "ss: 2022 first payload length mismatch",
                            )));
                        }
                        self.read_plain_pos = 0;
                        self.read_state = ReadState::Length {
                            buf: vec![0_u8; TCP_CHUNK_SIZE_LEN + self.cipher.tag_len()],
                            pos: 0,
                        };
                        if self.serve_read_plain(buf) {
                            return Poll::Ready(Ok(()));
                        }
                    }
                },
                ReadState::Length {
                    buf: encrypted_len,
                    pos,
                } => match poll_fill(&mut self.inner, cx, encrypted_len, pos, true)? {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(()) => {
                        if encrypted_len.is_empty() {
                            self.read_state = ReadState::Eof;
                            return Poll::Ready(Ok(()));
                        }
                        let key = self.read_key.as_ref().ok_or_else(|| {
                            io::Error::new(
                                io::ErrorKind::InvalidData,
                                "shadowsocks read key missing",
                            )
                        })?;
                        let expected_len = decrypt_tcp_chunk_length(
                            self.cipher,
                            key,
                            &mut self.read_nonce,
                            encrypted_len,
                        )
                        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
                        if let Some(salt) = self.legacy_response_salt.take() {
                            self.replay.check(&salt).map_err(|error| {
                                io::Error::new(io::ErrorKind::InvalidData, error)
                            })?;
                        }
                        self.read_state = ReadState::Payload {
                            expected_len,
                            buf: vec![0_u8; expected_len + self.cipher.tag_len()],
                            pos: 0,
                        };
                    }
                },
                ReadState::Payload {
                    expected_len,
                    buf: encrypted_payload,
                    pos,
                } => match poll_fill(&mut self.inner, cx, encrypted_payload, pos, false)? {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(()) => {
                        let key = self.read_key.as_ref().ok_or_else(|| {
                            io::Error::new(
                                io::ErrorKind::InvalidData,
                                "shadowsocks read key missing",
                            )
                        })?;
                        self.read_plain = decrypt_tcp_chunk_payload(
                            self.cipher,
                            key,
                            &mut self.read_nonce,
                            *expected_len,
                            encrypted_payload,
                        )
                        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
                        self.read_plain_pos = 0;
                        self.read_state = ReadState::Length {
                            buf: vec![0_u8; TCP_CHUNK_SIZE_LEN + self.cipher.tag_len()],
                            pos: 0,
                        };
                        if self.serve_read_plain(buf) {
                            return Poll::Ready(Ok(()));
                        }
                    }
                },
                ReadState::Eof => return Poll::Ready(Ok(())),
            }
        }
    }
}

fn poll_fill<S>(
    inner: &mut S,
    cx: &mut Context<'_>,
    buf: &mut Vec<u8>,
    pos: &mut usize,
    allow_clean_eof: bool,
) -> io::Result<Poll<()>>
where
    S: AsyncRead + Unpin,
{
    while *pos < buf.len() {
        let mut read_buf = ReadBuf::new(&mut buf[*pos..]);
        match Pin::new(&mut *inner).poll_read(cx, &mut read_buf) {
            Poll::Ready(Ok(())) if read_buf.filled().is_empty() => {
                if allow_clean_eof && *pos == 0 {
                    buf.clear();
                    return Ok(Poll::Ready(()));
                }
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "shadowsocks unexpected EOF",
                ));
            }
            Poll::Ready(Ok(())) => *pos += read_buf.filled().len(),
            Poll::Ready(Err(error)) => return Err(error),
            Poll::Pending => return Ok(Poll::Pending),
        }
    }
    Ok(Poll::Ready(()))
}

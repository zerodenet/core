use super::*;
impl<S> ShadowsocksAeadStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    pub(super) fn poll_flush_pending(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        while self.write_pos < self.write_buf.len() {
            match Pin::new(&mut self.inner).poll_write(cx, &self.write_buf[self.write_pos..]) {
                Poll::Ready(Ok(0)) => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "shadowsocks write zero",
                    )));
                }
                Poll::Ready(Ok(n)) => self.write_pos += n,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => return Poll::Pending,
            }
        }
        self.write_buf.clear();
        self.write_pos = 0;
        Poll::Ready(Ok(()))
    }

    pub(super) fn poll_write_encrypted(
        &mut self,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.poll_flush_pending(cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Pending => return Poll::Pending,
        }

        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        if let Some(state) = &mut self.legacy_write {
            let n = buf.len().min(65535);
            self.write_buf = buf[..n].to_vec();
            state.encrypt(&mut self.write_buf);
            self.write_pos = 0;
            return Poll::Ready(Ok(n));
        }
        let n = buf
            .len()
            .min(crate::shared::max_tcp_payload_len(self.cipher));

        // 2022 inbound: the first write emits the response salt + the
        // fixed-length response header chunk (nonce 0, which doubles as the
        // first length chunk) + the first payload chunk (nonce 1). Body
        // length+payload pairs continue from nonce 2 via encrypt_tcp_chunk.
        if self.is_2022 && self.write_response_header_pending {
            let header_plain = crate::shared::build_2022_response_fixed_header(
                crate::shared::now_unix_seconds(),
                &self.request_salt,
                n as u16,
            )
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            let enc_header = crate::shared::encrypt_tcp_2022_single_chunk(
                self.cipher,
                &self.write_key,
                &mut self.write_nonce,
                &header_plain,
            )
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            let enc_payload = crate::shared::encrypt_tcp_2022_single_chunk(
                self.cipher,
                &self.write_key,
                &mut self.write_nonce,
                &buf[..n],
            )
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;

            self.write_buf.clear();
            self.write_buf.extend_from_slice(&self.response_salt);
            self.write_buf.extend_from_slice(&enc_header);
            self.write_buf.extend_from_slice(&enc_payload);
            self.write_pos = 0;
            self.write_response_header_pending = false;
            return Poll::Ready(Ok(n));
        }

        self.write_buf = encrypt_tcp_chunk(
            self.cipher,
            &self.write_key,
            &mut self.write_nonce,
            &buf[..n],
        )
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        self.write_pos = 0;
        Poll::Ready(Ok(n))
    }
}

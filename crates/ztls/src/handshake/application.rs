//! Post-handshake TLS records and key transitions.
use super::*;
impl Tls13Connection {
    pub(super) fn process_app_data(&mut self) -> io::Result<()> {
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
                CONTENT_TYPE_HANDSHAKE => update = self.tickets.receive_record(plaintext, true)?,
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
    pub fn send_close_notify(&mut self) -> io::Result<()> {
        if let (Some(key), Some(iv)) = (&self.app_write_key, &self.app_write_iv) {
            RecordEncryptor::new(key, iv, &mut self.write_seq)
                .encrypt_close_notify(&mut self.ciphertext_write_buf)?;
        }
        Ok(())
    }
}

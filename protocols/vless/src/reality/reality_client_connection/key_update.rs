//! Post-handshake key transitions; shared TLS owns secret derivation.
use super::*;
impl RealityClientConnection {
    pub(super) fn receive_key_update(&mut self, requested: bool) -> io::Result<()> {
        let secret = self
            .read_secret
            .as_mut()
            .ok_or_else(|| io::Error::other("missing TLS read secret"))?;
        let (secret, key, iv) = secret.next_generation()?;
        self.read_secret = Some(secret);
        self.app_read_key = Some(key);
        self.app_read_iv = Some(iv);
        self.read_seq = 0;
        if requested {
            self.update_write_key(false)?;
        }
        Ok(())
    }

    /// Rotate the outbound key, optionally requesting a peer rotation.
    /// Pending application plaintext is sent after the update with the new key.
    pub fn update_write_key(&mut self, request_peer: bool) -> io::Result<()> {
        if self.is_handshaking() || self.received_close_notify || self.fatal_error.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "TLS key update requires an open application session",
            ));
        }
        let (secret, key, iv) = self
            .write_secret
            .as_mut()
            .ok_or_else(|| io::Error::other("missing TLS write secret"))?
            .next_generation()?;
        RecordEncryptor::new(
            self.app_write_key
                .as_ref()
                .ok_or_else(|| io::Error::other("missing TLS write key"))?,
            self.app_write_iv
                .as_ref()
                .ok_or_else(|| io::Error::other("missing TLS write IV"))?,
            &mut self.write_seq,
        )
        .encrypt_handshake(
            &[24, 0, 0, 1, u8::from(request_peer)],
            &mut self.ciphertext_write_buf,
        )?;
        self.write_secret = Some(secret);
        self.app_write_key = Some(key);
        self.app_write_iv = Some(iv);
        self.write_seq = 0;
        Ok(())
    }
}

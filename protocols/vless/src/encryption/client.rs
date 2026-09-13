// SPDX-License-Identifier: MPL-2.0
// VLESS Encryption wire behavior adapted from XTLS/Xray-core v26.3.27
// (d2758a023cd7f4174a5a5fa4ff66e487d4342ba0), proxy/vless/encryption.
use super::{
    config::{EncryptionConfig, Mask},
    crypto::{ctr, invalid, AeadState},
    keys::{self, random},
    padding::Padding,
    state::{ClientCache, ResumeGuard, Ticket},
    stream::EncryptionStream,
};
use ml_kem::EncodedSizeUser;
use std::{
    io,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use x25519_dalek::{PublicKey, StaticSecret};

#[derive(Clone)]
pub struct EncryptionClient {
    config: EncryptionConfig,
    cache: ClientCache,
}
impl core::fmt::Debug for EncryptionClient {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("EncryptionClient")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}
impl EncryptionClient {
    pub fn new(config: EncryptionConfig) -> io::Result<Self> {
        for public in &config.keys {
            keys::validate_public(public)?;
        }
        Ok(Self {
            config,
            cache: Arc::new(Mutex::new(None)),
        })
    }
    pub async fn handshake<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        stream: S,
    ) -> io::Result<EncryptionStream<S>> {
        tokio::time::timeout(Duration::from_secs(30), self.handshake_inner(stream))
            .await
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    "VLESS encryption handshake timeout",
                )
            })?
    }
    async fn handshake_inner<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        mut stream: S,
    ) -> io::Result<EncryptionStream<S>> {
        let iv = random::<16>();
        let (relays, nfs) =
            keys::client_relays(&self.config.keys, &iv, self.config.mask != Mask::Native)?;
        let mut hello = iv.to_vec();
        hello.extend(relays);
        let mut nfs_aead = AeadState::new(&iv, &nfs, true);
        let cached = if self.config.resume {
            self.cache
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone()
                .filter(|ticket| ticket.expiry > Instant::now())
        } else {
            None
        };
        if let Some(ticket) = cached {
            let mut united = ticket.key.to_vec();
            united.extend(nfs);
            hello.extend(nfs_aead.seal(&32u16.to_be_bytes(), &[])?);
            let encrypted_ticket = nfs_aead.seal(&ticket.bytes, &[])?;
            hello.extend(&encrypted_ticket);
            let send = AeadState::new(&encrypted_ticket, &united, true);
            let mut connection = EncryptionStream::new(stream, united, send, None);
            connection.prewrite(hello);
            connection.resume_guard = Some(ResumeGuard {
                cache: self.cache.clone(),
                key: ticket.key,
            });
            if self.config.mask == Mask::Random {
                connection.random = true;
                connection.out_mask = Some(ctr(&connection.key, &iv));
            }
            return Ok(connection);
        }
        let kem = keys::kem_secret(&random::<64>())?;
        let secret = StaticSecret::from(random::<32>());
        let mut public = kem.encapsulation_key().as_bytes().to_vec();
        public.extend(PublicKey::from(&secret).as_bytes());
        hello.extend(nfs_aead.seal(&1232u16.to_be_bytes(), &[])?);
        hello.extend(nfs_aead.seal(&public, &[])?);
        let padding = Padding::new(&self.config.padding);
        let padding_wire = padding.encode(&mut nfs_aead)?;
        padding.send(&mut stream, hello, padding_wire).await?;
        let mut response = vec![0; 1136];
        stream.read_exact(&mut response).await?;
        let response = nfs_aead.open_at(&[255; 12], &response, &[])?;
        let mut pfs = keys::decapsulate(&kem, &response[..1088])?.to_vec();
        pfs.extend(keys::exchange(&secret, &response[1088..1120])?);
        let mut united = pfs.clone();
        united.extend(nfs);
        let send = AeadState::new(&public, &united, true);
        let mut receive = AeadState::new(&response, &united, true);
        let mut ticket = [0; 32];
        stream.read_exact(&mut ticket).await?;
        let ticket: [u8; 16] = receive
            .open(&ticket, &[])?
            .try_into()
            .map_err(|_| invalid("invalid ticket length"))?;
        let seconds = u16::from_be_bytes([ticket[0], ticket[1]]);
        if self.config.resume && seconds > 0 {
            *self.cache.lock().unwrap_or_else(|e| e.into_inner()) = Some(Ticket {
                bytes: ticket,
                key: pfs.as_slice().try_into().unwrap(),
                expiry: Instant::now() + Duration::from_secs(u64::from(seconds)),
            });
        }
        let mut length = [0; 18];
        stream.read_exact(&mut length).await?;
        let length = receive.open(&length, &[])?;
        let length = usize::from(u16::from_be_bytes([length[0], length[1]]));
        let mut connection = EncryptionStream::new(stream, united, send, Some(receive));
        connection.peer_padding(length)?;
        if self.config.mask == Mask::Random {
            connection.random = true;
            connection.out_mask = Some(ctr(&connection.key, &iv));
            connection.in_mask = Some(ctr(&connection.key, &ticket));
        }
        Ok(connection)
    }
}

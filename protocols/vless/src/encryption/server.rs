// SPDX-License-Identifier: MPL-2.0
// VLESS Encryption wire behavior adapted from XTLS/Xray-core v26.3.27
// (d2758a023cd7f4174a5a5fa4ff66e487d4342ba0), proxy/vless/encryption.
use super::{
    config::{EncryptionConfig, Mask},
    crypto::{ctr, invalid, record_length, AeadState},
    keys::{self, random, ServerKey},
    padding::Padding,
    state::ServerCache,
    stream::EncryptionStream,
};
use rand::Rng;
use std::{
    io,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use x25519_dalek::{PublicKey, StaticSecret};

#[derive(Clone)]
pub struct EncryptionServer {
    inner: Arc<Server>,
}
struct Server {
    config: EncryptionConfig,
    keys: Vec<ServerKey>,
    cache: Mutex<ServerCache>,
    maintenance_started: std::sync::atomic::AtomicBool,
}
impl EncryptionServer {
    pub fn new(config: EncryptionConfig) -> io::Result<Self> {
        let keys = config
            .keys
            .iter()
            .map(|key| ServerKey::new(key))
            .collect::<io::Result<_>>()?;
        Ok(Self {
            inner: Arc::new(Server {
                config,
                keys,
                cache: Mutex::new(ServerCache::default()),
                maintenance_started: std::sync::atomic::AtomicBool::new(false),
            }),
        })
    }
    pub fn public_keys(&self) -> Vec<Vec<u8>> {
        self.inner
            .keys
            .iter()
            .map(|key| key.public().to_vec())
            .collect()
    }
    pub async fn handshake<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        stream: S,
    ) -> io::Result<EncryptionStream<S>> {
        self.start_maintenance();
        tokio::time::timeout(Duration::from_secs(30), self.handshake_inner(stream))
            .await
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    "VLESS decryption handshake timeout",
                )
            })?
    }
    fn start_maintenance(&self) {
        if !self.inner.config.resume {
            return;
        }
        if self
            .inner
            .maintenance_started
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            return;
        }
        let owner = Arc::downgrade(&self.inner);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;
                let Some(owner) = owner.upgrade() else { break };
                owner
                    .cache
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .prune();
            }
        });
    }
    async fn handshake_inner<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        mut stream: S,
    ) -> io::Result<EncryptionStream<S>> {
        let server = &self.inner;
        let length = server
            .keys
            .iter()
            .map(|key| key.wire_len() + 32)
            .sum::<usize>()
            - 32;
        let mut iv = [0; 16];
        stream.read_exact(&mut iv).await?;
        let mut relays = vec![0; length];
        stream.read_exact(&mut relays).await?;
        let nfs = keys::server_relays(
            &server.keys,
            &iv,
            &mut relays,
            server.config.mask != Mask::Native,
        )?;
        let mut nfs_aead = AeadState::new(&iv, &nfs, true);
        let mut encrypted_length = [0; 18];
        stream.read_exact(&mut encrypted_length).await?;
        let length = match nfs_aead.open(&encrypted_length, &[]) {
            Ok(length) => length,
            Err(_) => {
                nfs_aead = AeadState::new(&iv, &nfs, false);
                nfs_aead.open(&encrypted_length, &[])?
            }
        };
        let length = usize::from(u16::from_be_bytes([length[0], length[1]]));
        if length == 32 {
            return self.resume(stream, &iv, &nfs, &mut nfs_aead).await;
        }
        if length < 1232 {
            return Err(invalid("truncated encryption PFS exchange"));
        }
        let mut encrypted_public = vec![0; length];
        stream.read_exact(&mut encrypted_public).await?;
        let public = nfs_aead.open(&encrypted_public, &[])?;
        let (ciphertext, kem_key) = keys::encapsulate(&public[..1184])?;
        let secret = StaticSecret::from(random::<32>());
        let mut pfs = kem_key.to_vec();
        pfs.extend(keys::exchange(&secret, &public[1184..1216])?);
        let mut response = ciphertext;
        response.extend(PublicKey::from(&secret).as_bytes());
        let mut united = pfs.clone();
        united.extend(nfs);
        let mut send = AeadState::new(&response, &united, nfs_aead.aes);
        let receive = AeadState::new(&public[..1216], &united, nfs_aead.aes);
        let mut ticket = random::<16>();
        let (from, to) = server.config.seconds;
        let seconds = if to == 0 {
            (u32::from(from) * rand::rng().random_range(50..=100) / 100) as u16
        } else {
            rand::rng().random_range(from.min(to)..=from.max(to))
        };
        ticket[..2].copy_from_slice(&seconds.to_be_bytes());
        if seconds > 0 {
            server
                .cache
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(ticket, pfs.as_slice().try_into().unwrap(), from.max(to))?;
        }
        let mut hello = nfs_aead.seal_at(&[255; 12], &response, &[])?;
        hello.extend(send.seal(&ticket, &[])?);
        let padding = Padding::new(&server.config.padding);
        let padding_wire = padding.encode(&mut send)?;
        padding.send(&mut stream, hello, padding_wire).await?;
        stream.read_exact(&mut encrypted_length).await?;
        let length = nfs_aead.open(&encrypted_length, &[])?;
        let length = usize::from(u16::from_be_bytes([length[0], length[1]]));
        if length < 17 {
            return Err(invalid("invalid client padding length"));
        }
        let mut padding = vec![0; length];
        stream.read_exact(&mut padding).await?;
        nfs_aead.open(&padding, &[])?;
        let mut connection = EncryptionStream::new(stream, united, send, Some(receive));
        if server.config.mask == Mask::Random {
            connection.random = true;
            connection.out_mask = Some(ctr(&connection.key, &ticket));
            connection.in_mask = Some(ctr(&connection.key, &iv));
        }
        Ok(connection)
    }
    async fn resume<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        mut stream: S,
        iv: &[u8; 16],
        nfs: &[u8; 32],
        aead: &mut AeadState,
    ) -> io::Result<EncryptionStream<S>> {
        if !self.inner.config.resume {
            return Err(invalid("0-RTT is not enabled"));
        }
        let mut ciphertext = [0; 32];
        stream.read_exact(&mut ciphertext).await?;
        let ticket: [u8; 16] = aead
            .open(&ciphertext, &[])?
            .try_into()
            .map_err(|_| invalid("invalid resume ticket"))?;
        let pfs = self
            .inner
            .cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .resume(&ticket, *nfs)?;
        let Some(pfs) = pfs else {
            let length = rand::rng().random_range(1279..=2279);
            let mut noise = vec![0; length];
            loop {
                use rand::RngCore;
                rand::rng().fill_bytes(&mut noise);
                if record_length(noise[..5].try_into().unwrap()).is_err() {
                    break;
                }
            }
            stream.write_all(&noise).await?;
            stream.flush().await?;
            return Err(invalid("expired encryption ticket"));
        };
        let mut united = pfs.to_vec();
        united.extend(nfs);
        let random = random::<16>();
        let send = AeadState::new(&random, &united, aead.aes);
        let receive = AeadState::new(&ciphertext, &united, aead.aes);
        let mut connection = EncryptionStream::new(stream, united, send, Some(receive));
        connection.prewrite(random.to_vec());
        if self.inner.config.mask == Mask::Random {
            connection.random = true;
            connection.out_mask = Some(ctr(&connection.key, &random));
            connection.in_mask = Some(ctr(&connection.key, iv));
        }
        Ok(connection)
    }
}

use bytes::BytesMut;
use quinn_proto::crypto::{CryptoError, HeaderKey, KeyPair, Keys, PacketKey};
use ring::{aead, hkdf};

use super::{expand, CipherSuite, Version, TAG_LEN};

pub(super) fn keys_from_pair(
    version: Version,
    suite: CipherSuite,
    local: &[u8],
    remote: &[u8],
) -> Result<Keys, CryptoError> {
    let local = DirectionalKeys::derive(version, suite, local)?;
    let remote = DirectionalKeys::derive(version, suite, remote)?;
    Ok(Keys {
        header: KeyPair {
            local: Box::new(local.header),
            remote: Box::new(remote.header),
        },
        packet: KeyPair {
            local: Box::new(local.packet),
            remote: Box::new(remote.packet),
        },
    })
}

struct DirectionalKeys {
    header: RingHeaderKey,
    packet: AeadPacketKey,
}

impl DirectionalKeys {
    fn derive(version: Version, suite: CipherSuite, secret: &[u8]) -> Result<Self, CryptoError> {
        if secret.len() != suite.secret_len() {
            return Err(CryptoError);
        }
        let _ = version;
        let prk = hkdf::Prk::new_less_safe(suite.hkdf(), secret);
        let key = expand(&prk, b"quic key", suite.packet_algorithm().key_len())?;
        let iv = expand(&prk, b"quic iv", 12)?;
        let hp = expand(&prk, b"quic hp", suite.header_algorithm().key_len())?;
        let iv: [u8; 12] = iv.try_into().map_err(|_| CryptoError)?;
        Ok(Self {
            header: RingHeaderKey {
                key: aead::quic::HeaderProtectionKey::new(suite.header_algorithm(), &hp)
                    .map_err(|_| CryptoError)?,
            },
            packet: AeadPacketKey {
                key: aead::LessSafeKey::new(
                    aead::UnboundKey::new(suite.packet_algorithm(), &key)
                        .map_err(|_| CryptoError)?,
                ),
                iv,
                confidentiality_limit: suite.confidentiality_limit(),
                integrity_limit: suite.integrity_limit(),
            },
        })
    }
}

struct AeadPacketKey {
    key: aead::LessSafeKey,
    iv: [u8; 12],
    confidentiality_limit: u64,
    integrity_limit: u64,
}

impl AeadPacketKey {
    fn nonce(&self, packet: u64) -> aead::Nonce {
        let mut nonce = self.iv;
        for (left, right) in nonce[4..].iter_mut().zip(packet.to_be_bytes()) {
            *left ^= right;
        }
        aead::Nonce::assume_unique_for_key(nonce)
    }
}

impl PacketKey for AeadPacketKey {
    fn encrypt(&self, packet: u64, buf: &mut [u8], header_len: usize) {
        let (header, payload_and_tag) = buf.split_at_mut(header_len);
        let payload_len = payload_and_tag.len() - TAG_LEN;
        let (payload, tag_out) = payload_and_tag.split_at_mut(payload_len);
        let tag = self
            .key
            .seal_in_place_separate_tag(self.nonce(packet), aead::Aad::from(&*header), payload)
            .expect("QUIC packet encryption failed");
        tag_out.copy_from_slice(tag.as_ref());
    }

    fn decrypt(
        &self,
        packet: u64,
        header: &[u8],
        payload: &mut BytesMut,
    ) -> Result<(), CryptoError> {
        let plaintext = self
            .key
            .open_in_place(
                self.nonce(packet),
                aead::Aad::from(header),
                payload.as_mut(),
            )
            .map_err(|_| CryptoError)?;
        let len = plaintext.len();
        payload.truncate(len);
        Ok(())
    }

    fn tag_len(&self) -> usize {
        TAG_LEN
    }

    fn confidentiality_limit(&self) -> u64 {
        self.confidentiality_limit
    }

    fn integrity_limit(&self) -> u64 {
        self.integrity_limit
    }
}

struct RingHeaderKey {
    key: aead::quic::HeaderProtectionKey,
}

impl RingHeaderKey {
    fn apply(&self, pn_offset: usize, packet: &mut [u8], encrypted: bool) {
        let Some(sample) = packet.get(pn_offset + 4..pn_offset + 20) else {
            return;
        };
        let sample: [u8; 16] = sample.try_into().expect("fixed QUIC header sample");
        let mask = self
            .key
            .new_mask(&sample)
            .expect("fixed-length QUIC header sample");
        let first_bits = if packet.first().is_some_and(|first| first & 0x80 != 0) {
            0x0f
        } else {
            0x1f
        };
        let first_plain = if encrypted {
            packet[0] ^ (mask[0] & first_bits)
        } else {
            packet[0]
        };
        let pn_len = usize::from(first_plain & 0x03) + 1;
        packet[0] ^= mask[0] & first_bits;
        for (byte, mask) in packet[pn_offset..].iter_mut().zip(&mask[1..]).take(pn_len) {
            *byte ^= mask;
        }
    }
}

impl HeaderKey for RingHeaderKey {
    fn decrypt(&self, pn_offset: usize, packet: &mut [u8]) {
        self.apply(pn_offset, packet, true);
    }

    fn encrypt(&self, pn_offset: usize, packet: &mut [u8]) {
        self.apply(pn_offset, packet, false);
    }

    fn sample_size(&self) -> usize {
        self.key.algorithm().sample_len()
    }
}

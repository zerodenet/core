//! TLS 1.2 record adapters for explicitly selected compatibility suites.
use crate::crypto::cipher::*;
use crate::msgs::message::{
    InboundPlainMessage, OutboundOpaqueMessage, OutboundPlainMessage, PrefixedPayload,
};
use crate::suites::ConnectionTrafficSecrets;
use crate::Error;
use alloc::boxed::Box;
use rustls_legacy_crypto::{Algorithm, RecordKey};

pub(super) struct Cbc(pub(super) Algorithm);
impl Tls12AeadAlgorithm for Cbc {
    fn encrypter(&self, _: AeadKey, _: &[u8], _: &[u8]) -> Box<dyn MessageEncrypter> {
        <dyn MessageEncrypter>::invalid()
    }
    fn decrypter(&self, _: AeadKey, _: &[u8]) -> Box<dyn MessageDecrypter> {
        <dyn MessageDecrypter>::invalid()
    }
    fn mac_key_len(&self) -> usize {
        self.0.mac_len()
    }
    fn encrypter_with_mac(
        &self,
        key: AeadKey,
        _: &[u8],
        _: &[u8],
        mac: &[u8],
    ) -> Box<dyn MessageEncrypter> {
        Box::new(CbcRecord(
            RecordKey::new(self.0, key.as_ref(), mac).expect("key block matches CBC suite"),
        ))
    }
    fn decrypter_with_mac(&self, key: AeadKey, _: &[u8], mac: &[u8]) -> Box<dyn MessageDecrypter> {
        Box::new(CbcRecord(
            RecordKey::new(self.0, key.as_ref(), mac).expect("key block matches CBC suite"),
        ))
    }
    fn key_block_shape(&self) -> KeyBlockShape {
        KeyBlockShape {
            enc_key_len: self.0.key_len(),
            fixed_iv_len: 0,
            explicit_nonce_len: 0,
        }
    }
    fn extract_keys(
        &self,
        _: AeadKey,
        _: &[u8],
        _: &[u8],
    ) -> Result<ConnectionTrafficSecrets, UnsupportedOperationError> {
        Err(UnsupportedOperationError)
    }
}
struct CbcRecord(RecordKey);
impl MessageEncrypter for CbcRecord {
    fn encrypt(
        &mut self,
        msg: OutboundPlainMessage<'_>,
        seq: u64,
    ) -> Result<OutboundOpaqueMessage, Error> {
        let mut plain = PrefixedPayload::with_capacity(msg.payload.len());
        plain.extend_from_chunks(&msg.payload);
        let aad = make_tls12_aad(seq, msg.typ, msg.version, 0);
        let encrypted = self
            .0
            .seal(aad[..11].try_into().unwrap(), plain.as_ref())
            .map_err(|_| Error::EncryptError)?;
        let mut payload = PrefixedPayload::with_capacity(encrypted.len());
        payload.extend_from_slice(&encrypted);
        Ok(OutboundOpaqueMessage::new(msg.typ, msg.version, payload))
    }
    fn encrypted_payload_len(&self, len: usize) -> usize {
        self.0.encrypted_len(len)
    }
}
impl MessageDecrypter for CbcRecord {
    fn decrypt<'a>(
        &mut self,
        mut msg: InboundOpaqueMessage<'a>,
        seq: u64,
    ) -> Result<InboundPlainMessage<'a>, Error> {
        let aad = make_tls12_aad(seq, msg.typ, msg.version, 0);
        let plain = self
            .0
            .open(aad[..11].try_into().unwrap(), &msg.payload)
            .map_err(|_| Error::DecryptError)?;
        let n = plain.len();
        msg.payload[..n].copy_from_slice(&plain);
        Ok(msg.into_plain_message_range(0..n))
    }
}

//! AES-256-CBC/SHA-256, absent from AWS-LC's TLS AEAD descriptors.
//! AES and HMAC use AWS-LC. Verification evaluates every possible TLS padding
//! length, with public-length HMAC inputs and constant-time selection. This
//! avoids a secret-dependent HMAC length (Lucky Thirteen), at a fixed cost of
//! up to 256 HMAC evaluations per record. This legacy suite is opt-in only.
use super::RecordError;
use aws_lc_rs::{
    cipher::{self, DecryptingKey, DecryptionContext, EncryptingKey, UnboundCipherKey},
    hmac,
};
use subtle::{Choice, ConstantTimeEq, ConstantTimeLess};
use zeroize::Zeroizing;

pub(super) fn seal(key: &[u8], aad: &[u8; 11], plain: &[u8]) -> Result<Vec<u8>, RecordError> {
    let hmac_key = hmac::Key::new(hmac::HMAC_SHA256, &key[..32]);
    let tag = tag(&hmac_key, aad, plain);
    let mut body = Zeroizing::new(plain.to_vec());
    body.extend_from_slice(tag.as_ref());
    let pad = 16 - body.len() % 16;
    let padded = body.len() + pad;
    body.resize(padded, (pad - 1) as u8);
    let key = UnboundCipherKey::new(&cipher::AES_256, &key[32..]).map_err(|_| RecordError)?;
    let cipher = EncryptingKey::cbc(key).map_err(|_| RecordError)?;
    let context = cipher.encrypt(&mut body).map_err(|_| RecordError)?;
    let DecryptionContext::Iv128(iv) = context else {
        return Err(RecordError);
    };
    let mut out = iv.as_ref().to_vec();
    out.extend_from_slice(&body);
    Ok(out)
}
fn tag(key: &hmac::Key, aad: &[u8; 11], plain: &[u8]) -> hmac::Tag {
    let mut ctx = hmac::Context::with_key(key);
    ctx.update(aad);
    ctx.update(&(plain.len() as u16).to_be_bytes());
    ctx.update(plain);
    ctx.sign()
}
pub(super) fn open(
    key: &[u8],
    aad: &[u8; 11],
    record: &[u8],
) -> Result<Zeroizing<Vec<u8>>, RecordError> {
    if record.len() < 64 || (record.len() - 16) % 16 != 0 {
        return Err(RecordError);
    }
    let iv: [u8; 16] = record[..16].try_into().map_err(|_| RecordError)?;
    let key_cipher =
        UnboundCipherKey::new(&cipher::AES_256, &key[32..]).map_err(|_| RecordError)?;
    let cipher = DecryptingKey::cbc(key_cipher).map_err(|_| RecordError)?;
    let mut body = Zeroizing::new(record[16..].to_vec());
    cipher
        .decrypt(&mut body, DecryptionContext::Iv128(iv.into()))
        .map_err(|_| RecordError)?;
    let n = body.len();
    let last = body[n - 1];
    let pad = last as usize + 1;
    let mut valid_padding = Choice::from(1);
    for offset in 0..256.min(n) {
        let required = (offset as u64).ct_lt(&(pad as u64));
        valid_padding &= !required | body[n - 1 - offset].ct_eq(&last);
    }
    let hmac_key = hmac::Key::new(hmac::HMAC_SHA256, &key[..32]);
    let mut valid_mac = Choice::from(0);
    // Every length/index below depends only on the public ciphertext length
    // and public loop counter. Never branch on the decrypted padding byte.
    for candidate in 1..=256.min(n - 32) {
        let end = n - 32 - candidate;
        let expected = tag(&hmac_key, aad, &body[..end]);
        valid_mac |=
            (candidate as u64).ct_eq(&(pad as u64)) & expected.as_ref().ct_eq(&body[end..end + 32]);
    }
    if !bool::from(valid_padding & valid_mac) {
        return Err(RecordError);
    }
    let plain_len = n - 32 - pad;
    if plain_len > 16384 {
        return Err(RecordError);
    }
    body.truncate(plain_len);
    Ok(body)
}

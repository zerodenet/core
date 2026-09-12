//! RustCrypto primitives matching the fixed reference dependency versions.
use crate::CipherKind;
use ccm::consts::{U12, U16};
use chacha20poly1305::aead::{generic_array::GenericArray, AeadInPlace, KeyInit};
use zero_core::Error;
type Aes128Ccm = ccm::Ccm<aes::Aes128, U16, U12>;
type Aes256Ccm = ccm::Ccm<aes::Aes256, U16, U12>;
type Sm4Ccm = ccm::Ccm<sm4::Sm4, U16, U12>;
type Sm4Gcm = aes_gcm::AesGcm<sm4::Sm4, U12>;

fn apply<C: AeadInPlace + KeyInit>(
    key: &[u8],
    nonce: &[u8],
    data: &mut Vec<u8>,
    encrypt: bool,
) -> Result<(), Error> {
    let cipher = C::new_from_slice(key).map_err(|_| Error::Protocol("ss: invalid AEAD key"))?;
    let nonce = GenericArray::from_slice(nonce);
    if encrypt {
        cipher.encrypt_in_place(nonce, b"", data)
    } else {
        cipher.decrypt_in_place(nonce, b"", data)
    }
    .map_err(|_| Error::Protocol("ss: AEAD authentication failed"))
}
fn transform(
    cipher: CipherKind,
    key: &[u8],
    input_nonce: &[u8],
    input: &[u8],
    encrypt: bool,
) -> Result<Vec<u8>, Error> {
    let size = if cipher == CipherKind::XChacha20Poly1305 {
        24
    } else {
        12
    };
    if input_nonce.len() > size {
        return Err(Error::Protocol("ss: invalid AEAD nonce"));
    }
    let mut nonce = vec![0; size];
    nonce[..input_nonce.len()].copy_from_slice(input_nonce);
    let mut data = input.to_vec();
    match cipher {
        CipherKind::XChacha20Poly1305 => {
            apply::<chacha20poly1305::XChaCha20Poly1305>(key, &nonce, &mut data, encrypt)
        }
        CipherKind::Aes128Ccm => apply::<Aes128Ccm>(key, &nonce, &mut data, encrypt),
        CipherKind::Aes256Ccm => apply::<Aes256Ccm>(key, &nonce, &mut data, encrypt),
        CipherKind::Aes128GcmSiv => {
            apply::<aes_gcm_siv::Aes128GcmSiv>(key, &nonce, &mut data, encrypt)
        }
        CipherKind::Aes256GcmSiv => {
            apply::<aes_gcm_siv::Aes256GcmSiv>(key, &nonce, &mut data, encrypt)
        }
        CipherKind::Sm4Gcm => apply::<Sm4Gcm>(key, &nonce, &mut data, encrypt),
        CipherKind::Sm4Ccm => apply::<Sm4Ccm>(key, &nonce, &mut data, encrypt),
        _ => Err(Error::Protocol("ss: cipher is not an extra AEAD method")),
    }?;
    Ok(data)
}
pub(super) fn encrypt(
    cipher: CipherKind,
    key: &[u8],
    nonce: &[u8],
    input: &[u8],
) -> Result<Vec<u8>, Error> {
    transform(cipher, key, nonce, input, true)
}
pub(super) fn decrypt(
    cipher: CipherKind,
    key: &[u8],
    nonce: &[u8],
    input: &[u8],
) -> Result<Vec<u8>, Error> {
    transform(cipher, key, nonce, input, false)
}

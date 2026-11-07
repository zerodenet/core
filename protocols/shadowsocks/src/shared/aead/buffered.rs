//! Fixed-slice UDP AEAD: no packet allocation, append or hidden plaintext copy.
use crate::CipherKind;
use chacha20poly1305::aead::generic_array::typenum::Unsigned;
use chacha20poly1305::aead::{generic_array::GenericArray, AeadInPlace, KeyInit};
use zero_core::Error;

type Aes128Ccm = ccm::Ccm<aes::Aes128, ccm::consts::U16, ccm::consts::U12>;
type Aes256Ccm = ccm::Ccm<aes::Aes256, ccm::consts::U16, ccm::consts::U12>;
type Sm4Ccm = ccm::Ccm<sm4::Sm4, ccm::consts::U16, ccm::consts::U12>;
type Sm4Gcm = aes_gcm::AesGcm<sm4::Sm4, ccm::consts::U12>;

fn apply<C: AeadInPlace + KeyInit>(
    key: &[u8],
    nonce: &[u8],
    bytes: &mut [u8],
    tag: &mut [u8],
    encrypt: bool,
) -> Result<(), Error> {
    let cipher = C::new_from_slice(key).map_err(|_| Error::Protocol("ss: invalid AEAD key"))?;
    if nonce.len() != <C as chacha20poly1305::aead::AeadCore>::NonceSize::USIZE
        || tag.len() != <C as chacha20poly1305::aead::AeadCore>::TagSize::USIZE
    {
        return Err(Error::Protocol("ss: invalid AEAD layout"));
    }
    if encrypt {
        let actual = cipher
            .encrypt_in_place_detached(GenericArray::from_slice(nonce), b"", bytes)
            .map_err(|_| Error::Protocol("ss: encryption failed"))?;
        tag.copy_from_slice(&actual);
    } else {
        cipher
            .decrypt_in_place_detached(
                GenericArray::from_slice(nonce),
                b"",
                bytes,
                GenericArray::from_slice(tag),
            )
            .map_err(|_| Error::Protocol("ss: decryption failed"))?;
    }
    Ok(())
}

pub(crate) fn transform(
    cipher: CipherKind,
    key: &[u8],
    input_nonce: &[u8],
    bytes: &mut [u8],
    tag: &mut [u8],
    encrypt: bool,
) -> Result<(), Error> {
    // Optional XChaCha in SIP004 zero-extends its 12-byte UDP nonce exactly as
    // the existing extra backend. SIP022 ChaCha always requires all 24 bytes.
    let size = if cipher.is_2022_chacha() || cipher == CipherKind::XChacha20Poly1305 {
        24
    } else {
        12
    };
    if input_nonce.len() > size || (cipher.is_2022_chacha() && input_nonce.len() != size) {
        return Err(Error::Protocol("ss: invalid AEAD nonce"));
    }
    let mut nonce = [0; 24];
    nonce[..input_nonce.len()].copy_from_slice(input_nonce);
    let nonce = &nonce[..size];
    macro_rules! run {
        ($kind:ty) => {
            apply::<$kind>(key, nonce, bytes, tag, encrypt)
        };
    }
    match cipher {
        CipherKind::Aes128Gcm | CipherKind::Blake3Aes128Gcm => run!(aes_gcm::Aes128Gcm),
        CipherKind::Aes256Gcm | CipherKind::Blake3Aes256Gcm => run!(aes_gcm::Aes256Gcm),
        CipherKind::Chacha20Poly1305 => run!(chacha20poly1305::ChaCha20Poly1305),
        CipherKind::Blake3Chacha20Poly1305 | CipherKind::XChacha20Poly1305 => {
            run!(chacha20poly1305::XChaCha20Poly1305)
        }
        CipherKind::Blake3Chacha8Poly1305 => run!(chacha20poly1305::XChaCha8Poly1305),
        CipherKind::Aes128Ccm => run!(Aes128Ccm),
        CipherKind::Aes256Ccm => run!(Aes256Ccm),
        CipherKind::Aes128GcmSiv => run!(aes_gcm_siv::Aes128GcmSiv),
        CipherKind::Aes256GcmSiv => run!(aes_gcm_siv::Aes256GcmSiv),
        CipherKind::Sm4Gcm => run!(Sm4Gcm),
        CipherKind::Sm4Ccm => run!(Sm4Ccm),
        _ => Err(Error::Protocol("ss: invalid UDP AEAD method")),
    }
}

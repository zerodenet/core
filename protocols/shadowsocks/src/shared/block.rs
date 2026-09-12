use super::*;
#[cfg(all(feature = "crypto", feature = "blake3"))]
pub(super) fn encrypt_aes_2022_header(
    cipher: CipherKind,
    master_key: &[u8],
    header: &mut [u8; 16],
) -> Result<(), Error> {
    use aes::{
        cipher::{BlockEncrypt, KeyInit},
        Aes128, Aes256,
    };

    match cipher {
        CipherKind::Blake3Aes128Gcm => {
            let cipher = Aes128::new_from_slice(master_key)
                .map_err(|_| Error::Protocol("ss: invalid key"))?;
            cipher.encrypt_block(header.into());
            Ok(())
        }
        CipherKind::Blake3Aes256Gcm => {
            let cipher = Aes256::new_from_slice(master_key)
                .map_err(|_| Error::Protocol("ss: invalid key"))?;
            cipher.encrypt_block(header.into());
            Ok(())
        }
        _ => Err(Error::Protocol("ss: cipher is not a 2022 aes method")),
    }
}

#[cfg(all(feature = "crypto", feature = "blake3"))]
pub(super) fn decrypt_aes_2022_header(
    cipher: CipherKind,
    master_key: &[u8],
    header: &mut [u8; 16],
) -> Result<(), Error> {
    use aes::{
        cipher::{BlockDecrypt, KeyInit},
        Aes128, Aes256,
    };

    match cipher {
        CipherKind::Blake3Aes128Gcm => {
            let cipher = Aes128::new_from_slice(master_key)
                .map_err(|_| Error::Protocol("ss: invalid key"))?;
            cipher.decrypt_block(header.into());
            Ok(())
        }
        CipherKind::Blake3Aes256Gcm => {
            let cipher = Aes256::new_from_slice(master_key)
                .map_err(|_| Error::Protocol("ss: invalid key"))?;
            cipher.decrypt_block(header.into());
            Ok(())
        }
        _ => Err(Error::Protocol("ss: cipher is not a 2022 aes method")),
    }
}

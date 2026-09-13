// SPDX-License-Identifier: MPL-2.0
// REALITY behavior follows XTLS/REALITY 9234c772ba8f, as pinned by Xray-core v26.3.27.
//! X25519MLKEM768 wire encoding and secret composition (group 4588).
use super::hello::{invalid, shares};
use ml_kem::EncodedSizeUser;
use std::io;

pub(super) struct ClientKey {
    kem: crate::mlkem::KemSecret,
    private: [u8; 32],
}
impl Drop for ClientKey {
    fn drop(&mut self) {
        use zeroize::Zeroize;
        self.private.zeroize();
    }
}
pub(super) fn key_share(private: [u8; 32]) -> io::Result<(Box<ClientKey>, Vec<u8>)> {
    let kem = crate::mlkem::kem_secret(&crate::mlkem::random::<64>())?;
    let x25519 = x25519_dalek::PublicKey::from(&x25519_dalek::StaticSecret::from(private));
    let mut public = kem.encapsulation_key().as_bytes().to_vec();
    public.extend_from_slice(x25519.as_bytes());
    Ok((Box::new(ClientKey { kem, private }), public))
}

pub(super) fn client_secret(
    private: &[u8; 32],
    kem: Option<&ClientKey>,
    record: &[u8],
) -> io::Result<Vec<u8>> {
    let shares = shares(record, false)?;
    if shares.len() != 1 {
        return Err(invalid());
    }
    let (group, data) = shares[0];
    let mut secret = match (group, data.len()) {
        (29, 32) => Vec::new(),
        (4588, 1120) => {
            crate::mlkem::decapsulate(&kem.ok_or_else(invalid)?.kem, &data[..1088])?.to_vec()
        }
        _ => return Err(invalid()),
    };
    let public: [u8; 32] = data[data.len() - 32..].try_into().unwrap();
    let private = if group == 4588 {
        &kem.ok_or_else(invalid)?.private
    } else {
        private
    };
    secret.extend_from_slice(&super::reality_auth::perform_ecdh(private, &public)?);
    Ok(secret)
}

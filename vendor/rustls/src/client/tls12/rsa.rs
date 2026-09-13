//! Static RSA client key exchange. Certificate validation happens before entry;
//! the server's Finished proves possession of the certified RSA private key.
use crate::{crypto::SecureRandom, webpki::ParsedCertificate, Error};
use alloc::{vec, vec::Vec};
use aws_lc_rs::rsa::{Pkcs1PublicEncryptingKey, PublicEncryptingKey};
use pki_types::CertificateDer;
use zeroize::Zeroizing;

pub(super) struct Premaster {
    pub(super) encrypted: Vec<u8>,
    pub(super) premaster: Zeroizing<[u8; 48]>,
}
pub(super) fn prepare(
    cert: &CertificateDer<'_>,
    random: &dyn SecureRandom,
) -> Result<Premaster, Error> {
    let spki = ParsedCertificate::try_from(cert)?.subject_public_key_info();
    let public = PublicEncryptingKey::from_der(spki.as_ref())
        .map_err(|_| Error::General("invalid RSA encryption certificate".into()))?;
    let key = Pkcs1PublicEncryptingKey::new(public).map_err(|_| Error::EncryptError)?;
    let mut premaster = Zeroizing::new([0u8; 48]);
    random.fill(&mut premaster[2..])?;
    // ClientHello.legacy_version is TLS 1.2, including TLS 1.3-capable clients.
    premaster[..2].copy_from_slice(&[3, 3]);
    let mut encrypted = vec![0; key.ciphertext_size()];
    let n = key
        .encrypt(&*premaster, &mut encrypted)
        .map_err(|_| Error::EncryptError)?
        .len();
    encrypted.truncate(n);
    Ok(Premaster {
        encrypted,
        premaster,
    })
}

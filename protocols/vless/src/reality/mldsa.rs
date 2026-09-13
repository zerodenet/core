// SPDX-License-Identifier: MPL-2.0
// REALITY behavior follows XTLS/REALITY 9234c772ba8f, as pinned by Xray-core v26.3.27.
//! REALITY's optional ML-DSA-65 proof, bound to the Ed25519 key and both hellos.
use ml_dsa::{KeyExport, Keypair, MlDsa65, Signature, Signer, SigningKey, Verifier, VerifyingKey};
use ring::hmac;
use std::{io, sync::Arc};
use x509_parser::{certificate::X509Certificate, prelude::FromDer};

pub type Signing = Arc<SigningKey<MlDsa65>>;
pub type Verifying = Arc<VerifyingKey<MlDsa65>>;
pub fn signer(seed: &str) -> io::Result<Signing> {
    let seed = crate::validation::decode_reality_material::<32>(seed).map_err(invalid)?;
    Ok(Arc::new(SigningKey::from_seed(&seed.into())))
}
pub fn verifier(key: &str) -> io::Result<Verifying> {
    let key = crate::validation::decode_reality_material::<1952>(key).map_err(invalid)?;
    Ok(Arc::new(VerifyingKey::decode(&key.into())))
}
pub fn public_key(seed: &str) -> io::Result<String> {
    use base64::Engine;
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(signer(seed)?.verifying_key().to_bytes()))
}
fn message(auth: &[u8; 32], public: &[u8], hellos: &[u8]) -> hmac::Tag {
    let mut context = hmac::Context::with_key(&hmac::Key::new(hmac::HMAC_SHA512, auth));
    context.update(public);
    context.update(hellos);
    context.sign()
}
pub(super) fn sign(
    key: &Signing,
    auth: &[u8; 32],
    public: &[u8],
    hellos: &[u8],
) -> io::Result<Vec<u8>> {
    let signature: Signature<MlDsa65> = key
        .try_sign(message(auth, public, hellos).as_ref())
        .map_err(io::Error::other)?;
    Ok(signature.encode().to_vec())
}
pub(super) fn verify(
    key: &Verifying,
    certificate: &[u8],
    auth: &[u8; 32],
    hellos: &[u8],
) -> io::Result<()> {
    let (_, cert) = X509Certificate::from_der(certificate).map_err(io::Error::other)?;
    let extra = cert
        .extensions()
        .first()
        .ok_or_else(|| invalid("REALITY certificate omitted ML-DSA-65 proof"))?;
    let signature = Signature::<MlDsa65>::try_from(extra.value).map_err(io::Error::other)?;
    key.verify(
        message(
            auth,
            cert.public_key().subject_public_key.data.as_ref(),
            hellos,
        )
        .as_ref(),
        &signature,
    )
    .map_err(|_| {
        io::Error::new(
            io::ErrorKind::PermissionDenied,
            "REALITY ML-DSA-65 proof rejected",
        )
    })
}
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

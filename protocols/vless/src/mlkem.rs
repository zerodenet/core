//! Shared ML-KEM-768 key validation and operations for VLESS extensions.
use ml_kem::{kem::Decapsulate, EncapsulateDeterministic, EncodedSizeUser, KemCore, MlKem768};
use rand::RngCore;
use std::io;
fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.to_owned())
}
pub(crate) type KemSecret = <MlKem768 as KemCore>::DecapsulationKey;
pub(crate) type KemPublic = <MlKem768 as KemCore>::EncapsulationKey;
pub(crate) fn random<const N: usize>() -> [u8; N] {
    let mut bytes = [0; N];
    rand::rng().fill_bytes(&mut bytes);
    bytes
}
pub(crate) fn kem_secret(seed: &[u8]) -> io::Result<KemSecret> {
    if seed.len() != 64 {
        return Err(invalid("invalid ML-KEM seed"));
    }
    let d: [u8; 32] = seed[..32].try_into().unwrap();
    let z: [u8; 32] = seed[32..].try_into().unwrap();
    Ok(MlKem768::generate_deterministic(&d.into(), &z.into()).0)
}
pub(crate) fn kem_public(bytes: &[u8]) -> io::Result<KemPublic> {
    if bytes.len() != 1184 {
        return Err(invalid("invalid ML-KEM public key length"));
    }
    // FIPS 203 encapsulation-key modulus check. The library's decoder reduces
    // coefficients, so reject non-canonical encodings before constructing it.
    for chunk in bytes[..1152].chunks_exact(3) {
        let a = u16::from(chunk[0]) | (u16::from(chunk[1] & 15) << 8);
        let b = (u16::from(chunk[1]) >> 4) | (u16::from(chunk[2]) << 4);
        if a >= 3329 || b >= 3329 {
            return Err(invalid("non-canonical ML-KEM public key"));
        }
    }
    let encoded = bytes
        .try_into()
        .map_err(|_| invalid("invalid ML-KEM public key"))?;
    Ok(KemPublic::from_bytes(&encoded))
}
pub(crate) fn encapsulate(bytes: &[u8]) -> io::Result<(Vec<u8>, [u8; 32])> {
    let (ciphertext, key) = kem_public(bytes)?
        .encapsulate_deterministic(&random::<32>().into())
        .map_err(|_| invalid("ML-KEM encapsulation failed"))?;
    Ok((ciphertext.to_vec(), key.as_slice().try_into().unwrap()))
}
pub(crate) fn decapsulate(secret: &KemSecret, bytes: &[u8]) -> io::Result<[u8; 32]> {
    let encoded = bytes
        .try_into()
        .map_err(|_| invalid("invalid ML-KEM ciphertext length"))?;
    let key = secret
        .decapsulate(&encoded)
        .map_err(|_| invalid("ML-KEM decapsulation failed"))?;
    Ok(key.as_slice().try_into().unwrap())
}

use std::fmt::{self, Debug, Formatter};
use std::marker::PhantomData;
use std::sync::Arc;

use hpke::aead::{Aead, AeadCtxR, AeadCtxS, AesGcm128, AesGcm256, ChaCha20Poly1305};
use hpke::kdf::{HkdfSha384, HkdfSha512, Kdf as KdfTrait};
use hpke::kem::X25519HkdfSha256;
use hpke::{Deserializable, Kem as KemTrait, OpModeR, OpModeS, Serializable};
use rustls::crypto::hpke::{
    EncapsulatedSecret, Hpke, HpkeOpener, HpkePrivateKey, HpkePublicKey, HpkeSealer, HpkeSuite,
};
use rustls::internal::msgs::enums::{HpkeAead, HpkeKdf, HpkeKem};
use rustls::internal::msgs::handshake::HpkeSymmetricCipherSuite;

type X25519 = X25519HkdfSha256;

static X25519_SHA384_AES128: RustCryptoHpke<AesGcm128, HkdfSha384> =
    RustCryptoHpke::new(HpkeKdf::HKDF_SHA384, HpkeAead::AES_128_GCM);
static X25519_SHA384_AES256: RustCryptoHpke<AesGcm256, HkdfSha384> =
    RustCryptoHpke::new(HpkeKdf::HKDF_SHA384, HpkeAead::AES_256_GCM);
static X25519_SHA384_CHACHA: RustCryptoHpke<ChaCha20Poly1305, HkdfSha384> =
    RustCryptoHpke::new(HpkeKdf::HKDF_SHA384, HpkeAead::CHACHA20_POLY_1305);
static X25519_SHA512_AES128: RustCryptoHpke<AesGcm128, HkdfSha512> =
    RustCryptoHpke::new(HpkeKdf::HKDF_SHA512, HpkeAead::AES_128_GCM);
static X25519_SHA512_AES256: RustCryptoHpke<AesGcm256, HkdfSha512> =
    RustCryptoHpke::new(HpkeKdf::HKDF_SHA512, HpkeAead::AES_256_GCM);
static X25519_SHA512_CHACHA: RustCryptoHpke<ChaCha20Poly1305, HkdfSha512> =
    RustCryptoHpke::new(HpkeKdf::HKDF_SHA512, HpkeAead::CHACHA20_POLY_1305);

pub(in crate::tls) fn supported_suites() -> Vec<&'static dyn Hpke> {
    let mut suites = Vec::from(rustls::crypto::aws_lc_rs::hpke::ALL_SUPPORTED_SUITES);
    suites.extend([
        &X25519_SHA384_AES128 as &'static dyn Hpke,
        &X25519_SHA384_AES256,
        &X25519_SHA384_CHACHA,
        &X25519_SHA512_AES128,
        &X25519_SHA512_AES256,
        &X25519_SHA512_CHACHA,
    ]);
    suites
}

struct RustCryptoHpke<A, Kdf> {
    suite: HpkeSuite,
    marker: PhantomData<fn() -> (A, Kdf)>,
}

impl<A, Kdf> RustCryptoHpke<A, Kdf> {
    const fn new(kdf_id: HpkeKdf, aead_id: HpkeAead) -> Self {
        Self {
            suite: HpkeSuite {
                kem: HpkeKem::DHKEM_X25519_HKDF_SHA256,
                sym: HpkeSymmetricCipherSuite { kdf_id, aead_id },
            },
            marker: PhantomData,
        }
    }
}

impl<A, Kdf> Debug for RustCryptoHpke<A, Kdf> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        self.suite.fmt(formatter)
    }
}

impl<A, Kdf> Hpke for RustCryptoHpke<A, Kdf>
where
    A: Aead + 'static,
    Kdf: KdfTrait + 'static,
    AeadCtxS<A, Kdf, X25519>: Send + Sync,
    AeadCtxR<A, Kdf, X25519>: Send + Sync,
{
    fn seal(
        &self,
        info: &[u8],
        aad: &[u8],
        plaintext: &[u8],
        pub_key: &HpkePublicKey,
    ) -> Result<(EncapsulatedSecret, Vec<u8>), rustls::Error> {
        let (encapsulated, mut sealer) = self.setup_sealer(info, pub_key)?;
        Ok((encapsulated, sealer.seal(aad, plaintext)?))
    }

    fn setup_sealer(
        &self,
        info: &[u8],
        pub_key: &HpkePublicKey,
    ) -> Result<(EncapsulatedSecret, Box<dyn HpkeSealer + 'static>), rustls::Error> {
        let public_key =
            <<X25519 as KemTrait>::PublicKey as Deserializable>::from_bytes(&pub_key.0)
                .map_err(other)?;
        let (encapsulated, context) =
            hpke::setup_sender::<A, Kdf, X25519>(&OpModeS::Base, &public_key, info)
                .map_err(other)?;
        Ok((
            EncapsulatedSecret(encapsulated.to_bytes().to_vec()),
            Box::new(RustCryptoSealer(context)),
        ))
    }

    fn open(
        &self,
        enc: &EncapsulatedSecret,
        info: &[u8],
        aad: &[u8],
        ciphertext: &[u8],
        secret_key: &HpkePrivateKey,
    ) -> Result<Vec<u8>, rustls::Error> {
        self.setup_opener(enc, info, secret_key)?
            .open(aad, ciphertext)
    }

    fn setup_opener(
        &self,
        enc: &EncapsulatedSecret,
        info: &[u8],
        secret_key: &HpkePrivateKey,
    ) -> Result<Box<dyn HpkeOpener + 'static>, rustls::Error> {
        let private_key = <<X25519 as KemTrait>::PrivateKey as Deserializable>::from_bytes(
            secret_key.secret_bytes(),
        )
        .map_err(other)?;
        let encapsulated =
            <<X25519 as KemTrait>::EncappedKey as Deserializable>::from_bytes(&enc.0)
                .map_err(other)?;
        let context = hpke::setup_receiver::<A, Kdf, X25519>(
            &OpModeR::Base,
            &private_key,
            &encapsulated,
            info,
        )
        .map_err(other)?;
        Ok(Box::new(RustCryptoOpener(context)))
    }

    fn generate_key_pair(&self) -> Result<(HpkePublicKey, HpkePrivateKey), rustls::Error> {
        let (private_key, public_key) = X25519::gen_keypair();
        Ok((
            HpkePublicKey(public_key.to_bytes().to_vec()),
            HpkePrivateKey::from(private_key.to_bytes().to_vec()),
        ))
    }

    fn suite(&self) -> HpkeSuite {
        self.suite
    }
}

struct RustCryptoSealer<A: Aead, Kdf: KdfTrait>(AeadCtxS<A, Kdf, X25519>);

impl<A, Kdf> Debug for RustCryptoSealer<A, Kdf>
where
    A: Aead,
    Kdf: KdfTrait,
{
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("RustCryptoHpkeSealer")
    }
}

impl<A, Kdf> HpkeSealer for RustCryptoSealer<A, Kdf>
where
    A: Aead + 'static,
    Kdf: KdfTrait + 'static,
    AeadCtxS<A, Kdf, X25519>: Send + Sync,
{
    fn seal(&mut self, aad: &[u8], plaintext: &[u8]) -> Result<Vec<u8>, rustls::Error> {
        self.0.seal(plaintext, aad).map_err(other)
    }
}

struct RustCryptoOpener<A: Aead, Kdf: KdfTrait>(AeadCtxR<A, Kdf, X25519>);

impl<A, Kdf> Debug for RustCryptoOpener<A, Kdf>
where
    A: Aead,
    Kdf: KdfTrait,
{
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("RustCryptoHpkeOpener")
    }
}

impl<A, Kdf> HpkeOpener for RustCryptoOpener<A, Kdf>
where
    A: Aead + 'static,
    Kdf: KdfTrait + 'static,
    AeadCtxR<A, Kdf, X25519>: Send + Sync,
{
    fn open(&mut self, aad: &[u8], ciphertext: &[u8]) -> Result<Vec<u8>, rustls::Error> {
        self.0.open(ciphertext, aad).map_err(other)
    }
}

fn other(error: hpke::HpkeError) -> rustls::Error {
    rustls::Error::Other(rustls::OtherError(Arc::new(error)))
}

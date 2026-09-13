//! TLS groups absent from the default provider, composed from audited primitives.
use rustls::{
    crypto::{ActiveKeyExchange, CompletedKeyExchange, SharedSecret, SupportedKxGroup},
    ffdhe_groups::FfdheGroup,
    Error, NamedGroup, PeerMisbehaved, ProtocolVersion,
};

mod p521;

pub(super) fn lookup(id: u16) -> Option<&'static dyn SupportedKxGroup> {
    match id {
        25 => Some(&p521::P521),
        4589 => Some(&P384MlKem1024),
        _ => rustls::crypto::aws_lc_rs::ALL_KX_GROUPS
            .iter()
            .copied()
            .find(|group| u16::from(group.name()) == id),
    }
}

fn invalid_share() -> Error {
    Error::PeerMisbehaved(PeerMisbehaved::InvalidKeyShare)
}

// Go 1.26.1's SecP384r1MLKEM1024 uses classical || ML-KEM for
// both shares and secrets. Public key and ciphertext are each 1,568 bytes.
const CLASSICAL_LEN: usize = 97;
const MLKEM_LEN: usize = 1568;
const GROUP: NamedGroup = NamedGroup::Unknown(4589);

fn split(share: &[u8]) -> Result<(&[u8], &[u8]), Error> {
    if share.len() != CLASSICAL_LEN + MLKEM_LEN {
        return Err(invalid_share());
    }
    Ok(share.split_at(CLASSICAL_LEN))
}

#[derive(Debug)]
struct P384MlKem1024;
impl SupportedKxGroup for P384MlKem1024 {
    fn start(&self) -> Result<Box<dyn ActiveKeyExchange>, Error> {
        let classical = rustls::crypto::aws_lc_rs::kx_group::SECP384R1.start()?;
        let kem = rustls::crypto::aws_lc_rs::kx_group::MLKEM1024.start()?;
        let public = [classical.pub_key(), kem.pub_key()].concat();
        Ok(Box::new(HybridExchange {
            classical,
            kem,
            public,
        }))
    }

    fn start_and_complete(&self, share: &[u8]) -> Result<CompletedKeyExchange, Error> {
        let (classical, kem) = split(share)?;
        let classical =
            rustls::crypto::aws_lc_rs::kx_group::SECP384R1.start_and_complete(classical)?;
        let kem = rustls::crypto::aws_lc_rs::kx_group::MLKEM1024.start_and_complete(kem)?;
        Ok(CompletedKeyExchange {
            group: GROUP,
            pub_key: [classical.pub_key.as_slice(), kem.pub_key.as_slice()].concat(),
            secret: SharedSecret::from(
                [classical.secret.secret_bytes(), kem.secret.secret_bytes()].concat(),
            ),
        })
    }

    fn name(&self) -> NamedGroup {
        GROUP
    }
    fn ffdhe_group(&self) -> Option<FfdheGroup<'static>> {
        None
    }
    fn usable_for_version(&self, version: ProtocolVersion) -> bool {
        version == ProtocolVersion::TLSv1_3
    }
}

struct HybridExchange {
    classical: Box<dyn ActiveKeyExchange>,
    kem: Box<dyn ActiveKeyExchange>,
    public: Vec<u8>,
}
impl ActiveKeyExchange for HybridExchange {
    fn complete(self: Box<Self>, share: &[u8]) -> Result<SharedSecret, Error> {
        let (classical, kem) = split(share)?;
        let classical = self.classical.complete(classical)?;
        let kem = self.kem.complete(kem)?;
        Ok(SharedSecret::from(
            [classical.secret_bytes(), kem.secret_bytes()].concat(),
        ))
    }
    fn pub_key(&self) -> &[u8] {
        &self.public
    }
    fn group(&self) -> NamedGroup {
        GROUP
    }
    fn ffdhe_group(&self) -> Option<FfdheGroup<'static>> {
        None
    }
    fn hybrid_component(&self) -> Option<(NamedGroup, &[u8])> {
        Some((self.classical.group(), self.classical.pub_key()))
    }
    fn complete_hybrid_component(self: Box<Self>, share: &[u8]) -> Result<SharedSecret, Error> {
        self.classical.complete(share)
    }
}

#[cfg(test)]
#[path = "../../tests/tls/groups.rs"]
mod tests;

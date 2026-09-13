use crate::{
    crypto::{ActiveKeyExchange, SharedSecret, SupportedKxGroup},
    ffdhe_groups::FfdheGroup,
    Error, NamedGroup, PeerMisbehaved, ProtocolVersion,
};
use alloc::boxed::Box;
#[derive(Debug)]
struct Group(NamedGroup, usize);
/// RFC 7919 groups used by legacy browser profiles.
pub static FFDHE_GROUPS: &[&dyn SupportedKxGroup] = &[
    &Group(NamedGroup::FFDHE2048, 2048),
    &Group(NamedGroup::FFDHE3072, 3072),
    &Group(NamedGroup::FFDHE4096, 4096),
    &Group(NamedGroup::FFDHE8192, 8192),
];
fn invalid() -> Error {
    PeerMisbehaved::InvalidKeyShare.into()
}
impl SupportedKxGroup for Group {
    fn start(&self) -> Result<Box<dyn ActiveKeyExchange>, Error> {
        Ok(Box::new(Exchange {
            key: rustls_legacy_crypto::dh::Key::new(self.1).map_err(|_| invalid())?,
            group: self.0,
        }))
    }
    fn name(&self) -> NamedGroup {
        self.0
    }
    fn ffdhe_group(&self) -> Option<FfdheGroup<'static>> {
        Some(match self.0 {
            NamedGroup::FFDHE2048 => crate::ffdhe_groups::FFDHE2048,
            NamedGroup::FFDHE3072 => crate::ffdhe_groups::FFDHE3072,
            NamedGroup::FFDHE4096 => crate::ffdhe_groups::FFDHE4096,
            NamedGroup::FFDHE8192 => crate::ffdhe_groups::FFDHE8192,
            _ => return None,
        })
    }
    fn usable_for_version(&self, version: ProtocolVersion) -> bool {
        version == ProtocolVersion::TLSv1_2
    }
}
struct Exchange {
    key: rustls_legacy_crypto::dh::Key,
    group: NamedGroup,
}
impl ActiveKeyExchange for Exchange {
    fn pub_key(&self) -> &[u8] {
        self.key.public_key()
    }
    fn group(&self) -> NamedGroup {
        self.group
    }
    fn complete(self: Box<Self>, peer: &[u8]) -> Result<SharedSecret, Error> {
        let secret = self.key.complete(peer).map_err(|_| invalid())?;
        Ok(SharedSecret::from(secret.as_slice()))
    }
}

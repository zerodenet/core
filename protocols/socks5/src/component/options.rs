use alloc::sync::Arc;
use core::{
    fmt,
    hash::{BuildHasher, Hash, Hasher},
};
use std::{collections::hash_map::RandomState, sync::OnceLock};
use zero_core::{request::Destination, Error};
use zero_traits::DialPolicy;

/// Prepared once by the owning adapter; credentials never enter Debug/specs.
pub struct Options {
    pub(super) endpoint: Destination,
    pub(super) auth: Option<crate::Socks5OwnedOutboundAuth>,
    pub(super) dial: DialPolicy,
    pub(super) identity: Arc<[u8]>,
}
impl fmt::Debug for Options {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Options")
            .field("endpoint", &self.endpoint)
            .field("authenticated", &self.auth.is_some())
            .field("dial", &self.dial)
            .finish_non_exhaustive()
    }
}
impl Options {
    pub fn new(
        endpoint: Destination,
        username: Option<&str>,
        password: Option<&str>,
        dial: DialPolicy,
    ) -> Result<Self, Error> {
        if endpoint.port == 0 {
            return Err(Error::Config("server port is required"));
        }
        if matches!(&endpoint.address, zero_core::Address::Domain(host) if host.is_empty()) {
            return Err(Error::Config("server is required"));
        }
        let auth = match (username, password) {
            (None, None) => None,
            (Some(username), Some(password)) => {
                crate::validate_credential_part(username, "username")?;
                crate::validate_credential_part(password, "password")?;
                Some(crate::Socks5OwnedOutboundAuth::new(
                    crate::Socks5OutboundAuth { username, password },
                ))
            }
            _ => {
                return Err(Error::Config(
                    "`socks5` outbound requires both `username` and `password`, or neither",
                ))
            }
        };
        dial.validate()
            .map_err(|_| Error::Config("invalid SOCKS5 dial policy"))?;
        // Process-local keyed fingerprints are equality hints, never resource IDs
        // or persisted/public config. Host still compares the full source options.
        // Two independent secret keys avoid exposing a credential dictionary hash.
        static KEYS: OnceLock<[RandomState; 2]> = OnceLock::new();
        let mut identity = alloc::vec::Vec::with_capacity(16);
        for key in KEYS.get_or_init(|| [RandomState::new(), RandomState::new()]) {
            let mut hash = key.build_hasher();
            endpoint.hash(&mut hash);
            username.hash(&mut hash);
            password.hash(&mut hash);
            dial.hash(&mut hash);
            identity.extend_from_slice(&hash.finish().to_le_bytes());
        }
        Ok(Self {
            endpoint,
            auth,
            dial,
            identity: identity.into(),
        })
    }
}

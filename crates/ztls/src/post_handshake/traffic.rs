//! TLS 1.3 application traffic secrets, independent of connection direction.
use crate::{
    aead::AeadKey,
    cipher::CipherSuite,
    keys::{derive_traffic_keys, hkdf_expand_label_with_algorithm},
};
use std::io;
use zeroize::Zeroizing;

pub struct TrafficSecret {
    suite: CipherSuite,
    secret: Zeroizing<Vec<u8>>,
}
impl TrafficSecret {
    pub fn new(suite: CipherSuite, secret: Vec<u8>) -> Self {
        Self {
            suite,
            secret: Zeroizing::new(secret),
        }
    }

    /// Derive the next generation without changing the active record keys.
    /// Callers emit KeyUpdate under the old write key before installing these.
    pub fn next_generation(&self) -> io::Result<(Self, AeadKey, Vec<u8>)> {
        let next = Zeroizing::new(hkdf_expand_label_with_algorithm(
            self.suite.hmac_algorithm(),
            &self.secret,
            b"traffic upd",
            b"",
            self.suite.hmac_algorithm().digest_algorithm().output_len(),
        )?);
        let (key, iv) = derive_traffic_keys(&next, self.suite)?;
        let key = Zeroizing::new(key);
        let key = AeadKey::new(self.suite, &key)?;
        Ok((
            Self {
                suite: self.suite,
                secret: next,
            },
            key,
            iv,
        ))
    }
}

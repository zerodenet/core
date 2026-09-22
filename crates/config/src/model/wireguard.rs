use core::fmt;

use serde::{Deserialize, Serialize};
use zeroize::Zeroize;

/// A serialized WireGuard key whose diagnostic representation never exposes it.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WireguardSecret(String);

impl WireguardSecret {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for WireguardSecret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("WireguardSecret([REDACTED])")
    }
}

impl Drop for WireguardSecret {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

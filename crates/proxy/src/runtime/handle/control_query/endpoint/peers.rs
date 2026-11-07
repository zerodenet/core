//! Only original configured public peer fields and actual fixed observations.
use super::*;
#[cfg(feature = "wireguard")]
use serde::ser::{SerializeMap, SerializeSeq, SerializeStruct};
pub(super) struct Details<'a> {
    pub(super) facts: &'a EndpointDeviceRows,
    pub(super) config: &'a zero_config::RuntimeConfig,
}
#[cfg(feature = "wireguard")]
mod wireguard;
impl Serialize for Details<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        #[cfg(feature = "wireguard")]
        {
            return wireguard::Details {
                facts: &self.facts.wireguard,
                config: self.config,
            }
            .serialize(s);
        }
        #[cfg(not(feature = "wireguard"))]
        {
            let _ = self;
            Err(serde::ser::Error::custom(
                "endpoint protocol has no registered detail observer",
            ))
        }
    }
}

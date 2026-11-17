//! Only original configured public peer fields and actual fixed observations.
use super::*;
pub(super) struct Details<'a> {
    pub(super) facts: &'a EndpointDeviceRows,
    pub(super) config: &'a zero_config::RuntimeConfig,
}
impl Serialize for Details<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.facts.serialize_details(self.config, serializer)
    }
}

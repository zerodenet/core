use serde::{ser::SerializeStruct, Serialize, Serializer};

use super::RuntimeConfig;

impl Serialize for RuntimeConfig {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // Canonical resources own these role projections. Export their source
        // once so a saved configuration cannot bind the same resource twice.
        let inbounds: Vec<_> = self
            .inbounds
            .iter()
            .filter(|inbound| {
                !self.endpoints.iter().any(|endpoint| {
                    endpoint.listen.is_some() && endpoint.inbound_tag() == inbound.tag
                })
            })
            .collect();
        let outbounds: Vec<_> = self
            .outbounds
            .iter()
            .filter(|outbound| {
                !self
                    .endpoints
                    .iter()
                    .any(|endpoint| endpoint.tag == outbound.tag)
            })
            .collect();
        let mut state = serializer.serialize_struct("RuntimeConfig", 9)?;
        state.serialize_field("schema_version", &self.schema_version)?;
        state.serialize_field("endpoints", &self.endpoints)?;
        state.serialize_field("inbounds", &inbounds)?;
        state.serialize_field("outbounds", &outbounds)?;
        state.serialize_field("outbound_groups", &self.outbound_groups)?;
        state.serialize_field("runtime", &self.runtime)?;
        state.serialize_field("mode", &self.mode)?;
        state.serialize_field("route", &self.route)?;
        state.serialize_field("api", &self.api)?;
        state.end()
    }
}

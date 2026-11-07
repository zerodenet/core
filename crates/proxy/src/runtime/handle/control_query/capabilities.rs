//! Original compiled facts and build registration; all row storage is prepaid.
use super::super::query::capabilities::{host_sampling, visit_codes};
use super::*;
use serde::ser::{SerializeSeq, SerializeStruct};
use std::io;
use zero_runtime::configuration::ControlBudgetPermit;
use zero_traits::{
    ProtocolCapabilityDescriptor, ProtocolCapabilityState, ProtocolNetworkCapability,
};
#[derive(Debug)]
pub struct ProxyCapabilitiesQuery {
    protocols: Vec<ProtocolCapabilityDescriptor>,
    features: Vec<&'static str>,
    limitations: Vec<&'static str>,
    traffic: zero_api::BorrowedTrafficStatisticsCapability,
}
impl ProxyHandle {
    pub fn control_capabilities_query(
        &self,
        workspace: &mut ControlBudgetPermit,
    ) -> io::Result<ProxyCapabilitiesQuery> {
        let protocols = self
            .proxy
            .protocols
            .control_capability_descriptors(workspace)?;
        let endpoint_control = self.proxy.protocols.endpoint_control_available();
        let mut features_count = zero_engine::CONTROL_CAPABILITY_FEATURES.len();
        let mut limitations_count = 0usize;
        visit_codes(
            endpoint_control,
            |_| features_count += 1,
            |_| limitations_count += 1,
        );
        let bytes = features_count
            .checked_add(limitations_count)
            .and_then(|count| count.checked_mul(std::mem::size_of::<&'static str>()))
            .ok_or_else(|| io::Error::other("capability code workspace overflow"))?;
        workspace.grow(bytes)?;
        let mut features = Vec::new();
        features
            .try_reserve_exact(features_count)
            .map_err(io::Error::other)?;
        let mut limitations = Vec::new();
        limitations
            .try_reserve_exact(limitations_count)
            .map_err(io::Error::other)?;
        features.extend_from_slice(zero_engine::CONTROL_CAPABILITY_FEATURES);
        visit_codes(
            endpoint_control,
            |code| features.push(code),
            |code| limitations.push(code),
        );
        features.sort_unstable();
        features.dedup();
        limitations.sort_unstable();
        limitations.dedup();
        Ok(ProxyCapabilitiesQuery {
            protocols,
            features,
            limitations,
            traffic: zero_api::BorrowedTrafficStatisticsCapability::default()
                .with_automatic_sampling(true)
                .with_host_interface_sampling(host_sampling()),
        })
    }
}
impl ProxyCapabilitiesQuery {
    pub fn body(&self) -> impl Serialize + '_ {
        Body(self)
    }
}
impl Serialize for ProxyCapabilitiesQuery {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut out = s.serialize_map(Some(1))?;
        out.serialize_entry("capabilities", &Body(self))?;
        out.end()
    }
}
struct Body<'a>(&'a ProxyCapabilitiesQuery);
#[derive(Serialize)]
struct Channel {
    kind: &'static str,
    enabled: bool,
}
impl Serialize for Body<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut out = s.serialize_struct("ApiCapabilities", 12)?;
        out.serialize_field("traffic_statistics", &self.0.traffic)?;
        out.serialize_field("api_id", zero_api::API_ID)?;
        out.serialize_field("schema_id", zero_api::EVENT_SCHEMA_ID)?;
        out.serialize_field("contracts", &zero_api::ApiContractVersions::current())?;
        out.serialize_field("error_codes", &ErrorCodes)?;
        out.serialize_field("global_limitations", &self.0.limitations)?;
        out.serialize_field(
            "adapters",
            &[Channel {
                kind: "in_process",
                enabled: true,
            }],
        )?;
        out.serialize_field(
            "sinks",
            &[Channel {
                kind: "none",
                enabled: false,
            }],
        )?;
        out.serialize_field("features", &self.0.features)?;
        out.serialize_field("protocols", &Protocols(&self.0.protocols))?;
        out.serialize_field("build_features", zero_engine::control_build_features())?;
        out.serialize_field("permissions", &[zero_api::Permission::Read])?;
        out.end()
    }
}
struct ErrorCodes;
impl Serialize for ErrorCodes {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut out = s.serialize_seq(Some(zero_api::ApiErrorCode::ALL.len()))?;
        for code in zero_api::ApiErrorCode::ALL {
            out.serialize_element(code.as_code_str())?;
        }
        out.end()
    }
}
struct Protocols<'a>(&'a [ProtocolCapabilityDescriptor]);
impl Serialize for Protocols<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut out = s.serialize_seq(Some(self.0.len()))?;
        for original in self.0 {
            out.serialize_element(&Protocol(original))?;
        }
        out.end()
    }
}
struct Protocol<'a>(&'a ProtocolCapabilityDescriptor);
impl Serialize for Protocol<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let original = self.0;
        let mut out = s.serialize_struct("ProtocolCapability", 10)?;
        out.serialize_field("protocol", original.protocol)?;
        out.serialize_field("feature", original.feature)?;
        out.serialize_field("compiled", &true)?;
        out.serialize_field("status", original.status.as_str())?;
        out.serialize_field("compatibility_baseline", original.compatibility_baseline)?;
        out.serialize_field("inbound", &Network(original.inbound))?;
        out.serialize_field("outbound", &Network(original.outbound))?;
        out.serialize_field("transports", original.transports)?;
        out.serialize_field("mux", &State(original.mux))?;
        out.serialize_field("limitations", original.limitations)?;
        out.end()
    }
}
struct Network(ProtocolNetworkCapability);
impl Serialize for Network {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut out = s.serialize_struct("ProtocolNetworkCapability", 2)?;
        out.serialize_field("tcp", &State(self.0.tcp))?;
        out.serialize_field("udp", &State(self.0.udp))?;
        out.end()
    }
}
struct State(ProtocolCapabilityState);
impl Serialize for State {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut out = s.serialize_struct("CapabilityState", 3)?;
        out.serialize_field("supported", &self.0.supported)?;
        out.serialize_field("level", self.0.level.as_str())?;
        out.serialize_field("notes", self.0.notes)?;
        out.end()
    }
}
#[cfg(test)]
#[path = "tests/capabilities.rs"]
mod tests;

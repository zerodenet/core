//! Borrow identity/error/recovery/epoch values and serialize fixed scalar facts.
use super::*;
use serde::ser::{SerializeMap, SerializeSeq, SerializeStruct};
mod capabilities;
pub(super) struct Observed<'a> {
    pub(super) row: &'a EngineEndpointRow<'a>,
    pub(super) observation: Option<EndpointControlObservation>,
}
struct SourceFile<'a>(&'a EngineEndpointRow<'a>);
impl Serialize for SourceFile<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let row = self.0;
        let unavailable = if !row.intent.binding.canonical {
            Some("canonical_configuration_required")
        } else if !row.path {
            Some("source_path_unavailable")
        } else {
            None
        };
        let available = unavailable.is_none();
        let mut out = s.serialize_struct("EndpointSourceFileCapability", 4)?;
        out.serialize_field("available", &available)?;
        out.serialize_field(
            "writable",
            &available.then(|| row.source.writable()).flatten(),
        )?;
        out.serialize_field(
            "writable_observed_at_unix_ms",
            &available
                .then(|| row.source.observed_at_unix_ms())
                .flatten(),
        )?;
        out.serialize_field("reason", &unavailable.or_else(|| row.source.reason()))?;
        out.end()
    }
}
struct Configuration<'a>(&'a EngineEndpointRow<'a>);
impl Serialize for Configuration<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let row = self.0;
        let binding = &row.intent.binding;
        let mut out = s.serialize_struct("EndpointConfiguration", 4)?;
        out.serialize_field(
            "origin",
            &if binding.canonical {
                zero_api::EndpointConfigurationOrigin::Canonical
            } else {
                zero_api::EndpointConfigurationOrigin::Legacy
            },
        )?;
        out.serialize_field("enabled", &binding.enabled)?;
        out.serialize_field("directions", &binding.directions)?;
        out.serialize_field("source_file", &SourceFile(row))?;
        out.end()
    }
}
impl Serialize for Observed<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let row = self.row;
        let binding = &row.intent.binding;
        let mut state = row
            .fact
            .map_or(EndpointRuntimeState::Stopped, |fact| fact.state);
        let mut health = EndpointHealthState::Unknown;
        let mut generation = row.generation();
        let mut effective = EndpointDirections::default();
        if let Some(observed) = self.observation {
            if !matches!(
                state,
                EndpointRuntimeState::Starting
                    | EndpointRuntimeState::Stopping
                    | EndpointRuntimeState::Failed
            ) {
                state = observed.state;
            }
            health = observed.health;
            generation = observed.generation.or(generation);
            if row.intent.enabled() && state == EndpointRuntimeState::Running {
                let allowed = row.intent.directions();
                effective = EndpointDirections {
                    inbound: allowed.inbound && binding.supported_directions.inbound,
                    outbound: allowed.outbound && binding.supported_directions.outbound,
                };
            }
        }
        let mut out = s.serialize_map(None)?;
        out.serialize_entry("endpoint_id", &binding.endpoint_id)?;
        out.serialize_entry("tag", &binding.tag)?;
        out.serialize_entry("protocol", &binding.protocol)?;
        out.serialize_entry("inbound_tags", &binding.inbound_tags)?;
        out.serialize_entry("outbound_tags", &binding.outbound_tags)?;
        out.serialize_entry("supported", &capabilities::Capabilities(self))?;
        out.serialize_entry("configuration", &Configuration(row))?;
        out.serialize_entry("enabled", &row.intent.enabled())?;
        out.serialize_entry("allowed", &row.intent.directions())?;
        out.serialize_entry("effective", &effective)?;
        out.serialize_entry("state", &state)?;
        out.serialize_entry("health", &health)?;
        out.serialize_entry("state_source", &row.intent.source())?;
        out.serialize_entry("core_instance_id", row.core)?;
        out.serialize_entry("config_revision", &row.revision)?;
        out.serialize_entry("intent_revision", &row.intent.revision)?;
        out.serialize_entry("generation", &generation)?;
        out.serialize_entry("observed_at_unix_ms", &row.observed)?;
        out.serialize_entry(
            "started_at_unix_ms",
            &row.fact.and_then(|fact| fact.started_at_unix_ms),
        )?;
        out.serialize_entry("counters", &row.counters)?;
        out.serialize_entry(
            "stats_epoch",
            &row.traffic.as_ref().map(EndpointTrafficRow::epoch),
        )?;
        out.serialize_entry(
            "stats_epoch_started_at_unix_ms",
            &row.traffic.as_ref().map(EndpointTrafficRow::started),
        )?;
        if let Some(recovery) = row.fact.and_then(|fact| fact.recovery.as_ref()) {
            out.serialize_entry("recovery", recovery)?;
        }
        out.serialize_entry(
            "last_error",
            &row.fact.and_then(|fact| fact.last_error.as_ref()),
        )?;
        out.end()
    }
}
pub(super) struct Body<'a>(pub(super) &'a EngineEndpointRows<'a>);
struct Rows<'a>(&'a EngineEndpointRows<'a>);
impl Serialize for Rows<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut out = s.serialize_seq(Some(self.0.rows.len()))?;
        for row in &self.0.rows {
            out.serialize_element(row)?;
        }
        out.end()
    }
}
impl Serialize for Body<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        if !self.0.list {
            return self.0.rows[0].serialize(s);
        }
        let mut out = s.serialize_struct("EndpointListSnapshot", 3)?;
        out.serialize_field("endpoints", &Rows(self.0))?;
        out.serialize_field("total", &self.0.total)?;
        out.serialize_field("next_offset", &self.0.next)?;
        out.end()
    }
}

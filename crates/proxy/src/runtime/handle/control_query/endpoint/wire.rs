//! Same endpoint wire over already frozen metadata, not owned DTOs.
use super::*;
use serde::ser::{SerializeSeq, SerializeStruct};
pub(super) struct Body<'a>(pub(super) &'a ProxyEndpointQuery<'a>);
struct Row<'a> {
    row: &'a zero_engine::EngineEndpointRow<'a>,
    observed: Option<&'a Observation>,
}
impl Serialize for Row<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self.observed {
            Some(observed) => self.row.observed(observed.projection).serialize(s),
            None => self.row.serialize(s),
        }
    }
}
struct Rows<'a>(&'a ProxyEndpointQuery<'a>);
impl Serialize for Rows<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let view = self.0;
        let mut out = s.serialize_seq(Some(view.observations.len()))?;
        for (row, observed) in view.rows.rows().zip(&view.observations) {
            out.serialize_element(&Row {
                row,
                observed: observed.as_ref(),
            })?;
        }
        out.end()
    }
}
impl Serialize for Body<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let view = self.0;
        if view.details {
            let row = view.rows.rows().next().expect("same selected endpoint");
            let observed = view.observations[0]
                .as_ref()
                .expect("actual registered detail observer");
            let mut out = s.serialize_struct("EndpointDetailsSnapshot", 8)?;
            out.serialize_field("endpoint_id", &row.binding().endpoint_id)?;
            out.serialize_field("core_instance_id", row.core_instance_id())?;
            out.serialize_field("config_revision", &row.config_revision())?;
            out.serialize_field("observed_at_unix_ms", &row.observed_at_unix_ms())?;
            out.serialize_field(
                "generation",
                &observed.projection.generation.or(row.generation()),
            )?;
            out.serialize_field("schema_id", "zero.endpoint.wireguard.v1")?;
            out.serialize_field("schema_version", &1u32)?;
            out.serialize_field(
                "details",
                &peers::Details {
                    facts: &observed.facts,
                    config: view.config,
                },
            )?;
            return out.end();
        }
        if !view.rows.is_list() {
            return Row {
                row: view.rows.rows().next().expect("same selected endpoint"),
                observed: view.observations[0].as_ref(),
            }
            .serialize(s);
        }
        let mut out = s.serialize_struct("EndpointListSnapshot", 3)?;
        out.serialize_field("endpoints", &Rows(view))?;
        out.serialize_field("total", &view.rows.total())?;
        out.serialize_field("next_offset", &view.rows.next_offset())?;
        out.end()
    }
}

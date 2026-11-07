//! Serialize actual borrowed identities/epochs and the one frozen scalar capture.
use super::*;
use serde::{
    ser::{SerializeSeq, SerializeStruct},
    Serialize, Serializer,
};
struct Planes<'a>(&'a TrafficRow<'a>);
impl Serialize for Planes<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut out = s.serialize_seq(None)?;
        if matches!(self.0.scope, TrafficScope::HostInterface { .. }) {
            out.serialize_element(&self.0.planes[3])?;
        } else {
            for plane in &self.0.planes[..3] {
                out.serialize_element(plane)?;
            }
        }
        out.end()
    }
}
struct Snapshot<'a> {
    row: &'a TrafficRow<'a>,
    core: &'a str,
    revision: u64,
}
impl Serialize for Snapshot<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let r = self.row;
        let mut out = s.serialize_struct("TrafficSnapshot", 14)?;
        out.serialize_field("scope", r.scope)?;
        out.serialize_field("core_instance_id", self.core)?;
        out.serialize_field("config_revision", &self.revision)?;
        out.serialize_field("generation", &r.generation)?;
        out.serialize_field("stats_epoch", &r.period.epoch)?;
        out.serialize_field("epoch_started_at_unix_ms", &r.period.started)?;
        out.serialize_field("capture_started_at_unix_ms", &r.started)?;
        out.serialize_field("sampled_at_monotonic_ns", &r.monotonic)?;
        out.serialize_field("sampled_at_unix_ms", &r.sampled)?;
        out.serialize_field("planes", &Planes(r))?;
        out.serialize_field("activity", &r.activity)?;
        out.serialize_field("reset_policy", "all_available_cumulative_no_cascade")?;
        out.serialize_field("source_sampled_at_unix_ms", &r.source_time)?;
        out.serialize_field("source_sampled_at_monotonic_ns", &r.source_monotonic)?;
        out.end()
    }
}
struct Rows<'a>(&'a TrafficQueryRows<'a>);
impl Serialize for Rows<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut out = s.serialize_seq(Some(self.0.rows.len()))?;
        for row in &self.0.rows {
            out.serialize_element(&Snapshot {
                row,
                core: self.0.core,
                revision: self.0.revision,
            })?;
        }
        out.end()
    }
}
impl Serialize for TrafficQueryRows<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        if !self.list {
            return Snapshot {
                row: &self.rows[0],
                core: self.core,
                revision: self.revision,
            }
            .serialize(s);
        }
        let mut out = s.serialize_struct("TrafficListSnapshot", 7)?;
        out.serialize_field("core_instance_id", self.core)?;
        out.serialize_field("config_revision", &self.revision)?;
        out.serialize_field("registry_revision", &self.registry_revision)?;
        out.serialize_field("sampled_at_unix_ms", &self.sampled)?;
        out.serialize_field("scopes", &Rows(self))?;
        out.serialize_field("total", &self.total)?;
        out.serialize_field("next_offset", &self.next)?;
        out.end()
    }
}

//! Freeze actual device scalars only after paying their original row storage.
use super::*;
use crate::adapters::devices::HealthRow;
use serde::ser::{SerializeSeq, SerializeStruct};
use std::{io, sync::Arc};
use zero_runtime::configuration::ControlBudgetPermit;

pub struct ProxyHealthQuery<'a> {
    engine: &'a zero_engine::Engine,
    snapshot: Arc<zero_engine::EngineRuntimeSnapshot>,
    rows: Vec<HealthRow>,
}
impl std::fmt::Debug for ProxyHealthQuery<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProxyHealthQuery").finish_non_exhaustive()
    }
}
impl ProxyHandle {
    pub fn control_health_query(
        &self,
        workspace: &mut ControlBudgetPermit,
    ) -> io::Result<ProxyHealthQuery<'_>> {
        let engine = self.proxy.engine();
        let snapshot = engine.runtime_snapshot();
        #[cfg(feature = "raw-ip-runtime")]
        let count = self
            .proxy
            .protocols
            .device_health_row_count(snapshot.config())?;
        #[cfg(not(feature = "raw-ip-runtime"))]
        let count: usize = 0;
        let bytes = count
            .checked_mul(std::mem::size_of::<HealthRow>())
            .ok_or_else(|| io::Error::other("device health workspace size overflow"))?;
        workspace.grow(bytes)?;
        let mut rows = Vec::new();
        rows.try_reserve_exact(count).map_err(io::Error::other)?;
        #[cfg(feature = "raw-ip-runtime")]
        self.proxy
            .protocols
            .fill_device_health_rows(snapshot.config(), &mut rows);
        Ok(ProxyHealthQuery {
            engine,
            snapshot,
            rows,
        })
    }
}
impl ProxyHealthQuery<'_> {
    pub fn body(&self) -> impl Serialize + '_ {
        Body(self)
    }
}
impl Serialize for ProxyHealthQuery<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut out = s.serialize_map(Some(1))?;
        out.serialize_entry("health", &Body(self))?;
        out.end()
    }
}
struct Body<'a>(&'a ProxyHealthQuery<'a>);
impl Serialize for Body<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let view = self.0;
        let mut out =
            s.serialize_struct("HealthSnapshot", if view.rows.is_empty() { 5 } else { 6 })?;
        out.serialize_field("engine_build_id", env!("CARGO_PKG_VERSION"))?;
        out.serialize_field("core_instance_id", view.engine.core_instance_id())?;
        out.serialize_field("config_revision", &view.snapshot.config_revision())?;
        out.serialize_field("started_at_unix_ms", &view.engine.started_at_unix_ms())?;
        out.serialize_field("healthy", &true)?;
        if !view.rows.is_empty() {
            out.serialize_field("outbound_devices", &Rows(view))?;
        }
        out.end()
    }
}
struct Rows<'a>(&'a ProxyHealthQuery<'a>);
impl Serialize for Rows<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut out = s.serialize_seq(Some(self.0.rows.len()))?;
        for row in &self.0.rows {
            out.serialize_element(&Row {
                tag: &self.0.snapshot.config().outbounds[row.outbound].tag,
                facts: &row.facts,
            })?;
        }
        out.end()
    }
}
struct Row<'a> {
    tag: &'a str,
    facts: &'a zero_api::OutboundDeviceHealthSnapshot,
}
impl Serialize for Row<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let facts = self.facts;
        let mut out = s.serialize_map(None)?;
        out.serialize_entry("tag", self.tag)?;
        out.serialize_entry("peer_index", &facts.peer_index)?;
        out.serialize_entry("state", &facts.state)?;
        if let Some(age) = facts.last_handshake_age_ms {
            out.serialize_entry("last_handshake_age_ms", &age)?;
        }
        if let Some(age) = facts.last_authenticated_packet_age_ms {
            out.serialize_entry("last_authenticated_packet_age_ms", &age)?;
        }
        out.serialize_entry(
            "endpoint_resolution_failed",
            &facts.endpoint_resolution_failed,
        )?;
        out.end()
    }
}
#[cfg(all(test, feature = "wireguard"))]
#[path = "tests/health.rs"]
mod tests;

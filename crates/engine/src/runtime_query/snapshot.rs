//! Initial flow envelope over the original membership and field guards.
use super::*;
use crate::observability::SnapshotBasis;
use serde::ser::{SerializeSeq, SerializeStruct};

pub struct EngineFlowSnapshotGuard<'a> {
    active: SessionQueryGuard<'a>,
    basis: SnapshotBasis<'a>,
}
pub struct EngineFlowSnapshot<'a> {
    guard: &'a EngineFlowSnapshotGuard<'a>,
    active: ActiveQueryRows<'a>,
    principals: crate::session::PrincipalQueryRows<'a>,
}
impl Engine {
    /// Subscribe the live receiver first, then measure/prepay/encode this
    /// original envelope synchronously. No guard may cross await or refund.
    pub fn control_flow_snapshot(&self) -> EngineFlowSnapshotGuard<'_> {
        let (active, _, _) = self.query_data_roots();
        let active = active.query_guard();
        let basis = self.control_snapshot_basis();
        EngineFlowSnapshotGuard { active, basis }
    }
}
impl EngineFlowSnapshotGuard<'_> {
    pub fn freeze<'a>(
        &'a self,
        credit: &mut ControlBudgetPermit,
    ) -> io::Result<EngineFlowSnapshot<'a>> {
        let active = self.active.freeze(None, usize::MAX, None, credit)?;
        let principals = self.active.principals(credit)?;
        Ok(EngineFlowSnapshot {
            guard: self,
            active,
            principals,
        })
    }
}
struct Identity<'a>(&'a SnapshotBasis<'a>);
impl std::fmt::Display for Identity<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let b = self.0;
        write!(
            f,
            "{:032x}:{}:{}:{}",
            b.epoch,
            zero_api::event_type::FLOW_SNAPSHOT,
            b.watermark,
            b.identity
        )
    }
}
impl Serialize for Identity<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}
impl EngineFlowSnapshot<'_> {
    pub fn event_type(&self) -> &'static str {
        zero_api::event_type::FLOW_SNAPSHOT
    }
    pub fn event_id(&self) -> impl std::fmt::Display + '_ {
        Identity(&self.guard.basis)
    }
    pub fn occurred_at_unix_ms(&self) -> u64 {
        self.guard.basis.occurred_at
    }
    pub fn sequence(&self) -> u64 {
        self.guard.basis.watermark
    }
    pub fn payload(&self) -> impl Serialize + '_ {
        Payload(self)
    }
}
struct Records<'a>(&'a ActiveQueryRows<'a>);
impl Serialize for Records<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut records = s.serialize_seq(Some(self.0.len()))?;
        for row in self.0.iter() {
            records.serialize_element(&record::Record(row))?;
        }
        records.end()
    }
}
struct Payload<'a>(&'a EngineFlowSnapshot<'a>);
impl Serialize for Payload<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let v = self.0;
        let mut payload = s.serialize_struct("FlowSnapshot", 4)?;
        payload.serialize_field("watermark", &v.sequence())?;
        payload.serialize_field("session_registry_revision", &v.principals.revision)?;
        payload.serialize_field("principal_flows", &v.principals)?;
        payload.serialize_field("records", &Records(&v.active))?;
        payload.end()
    }
}
struct Labels;
impl Serialize for Labels {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_map(Some(0))?.end()
    }
}
impl Serialize for EngineFlowSnapshot<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let basis = &self.guard.basis;
        let mut event = s.serialize_struct("ApiEvent", 11)?;
        event.serialize_field("schema_id", zero_api::EVENT_SCHEMA_ID)?;
        event.serialize_field("event_id", &Identity(basis))?;
        event.serialize_field("event_type", self.event_type())?;
        event.serialize_field("occurred_at_unix_ms", &basis.occurred_at)?;
        event.serialize_field("core_instance_id", basis.core_instance_id)?;
        event.serialize_field("config_revision", &basis.config_revision)?;
        event.serialize_field("source_id", &None::<&str>)?;
        event.serialize_field("sequence", &basis.watermark)?;
        event.serialize_field("principal_key", &None::<&str>)?;
        event.serialize_field("labels", &Labels)?;
        event.serialize_field("payload", &self.payload())?;
        event.end()
    }
}
#[cfg(test)]
mod tests;

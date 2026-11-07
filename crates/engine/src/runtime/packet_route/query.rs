//! Freeze route membership/state over the actual originals before encoding.
use super::*;
use serde::{ser::SerializeSeq, Serialize, Serializer};
use std::{io, sync::MutexGuard};
use zero_runtime::configuration::ControlBudgetPermit;

pub(crate) struct PacketQueryGuard<'a> {
    entries: MutexGuard<'a, BTreeMap<String, Weak<Control>>>,
    request: &'a QueryRequest,
    engine: &'a Engine,
}
struct Row {
    original: Arc<Control>,
    closing: bool,
}
pub(crate) struct PacketQueryRows<'a> {
    rows: Vec<Row>,
    core_instance_id: &'a str,
    config_revision: u64,
    observed_at_unix_ms: u64,
    total: usize,
    next_offset: Option<usize>,
    single: bool,
}
impl Engine {
    pub(crate) fn packet_query<'a>(&'a self, request: &'a QueryRequest) -> PacketQueryGuard<'a> {
        PacketQueryGuard {
            entries: self
                .packet_routes
                .entries
                .lock()
                .unwrap_or_else(|e| e.into_inner()),
            request,
            engine: self,
        }
    }
}
impl PacketQueryGuard<'_> {
    pub(crate) fn freeze<'a>(
        &'a self,
        workspace: &mut ControlBudgetPermit,
    ) -> io::Result<PacketQueryRows<'a>> {
        let (offset, limit, single) = match self.request {
            QueryRequest::PacketRoutes(q) => {
                (q.offset, q.limit.unwrap_or(100).clamp(1, 1000), false)
            }
            QueryRequest::PacketRoute(_) => (0, 1, true),
            _ => unreachable!("selected packet route owner"),
        };
        // Signals can settle while membership is locked. Prepay the bounded
        // upper count, then observe each actual original exactly once; two
        // passes must not turn concurrent release into unchecked Vec growth.
        let mut rows = crate::runtime_query::workspace::vector::<Row>(
            self.entries.len().min(limit),
            workspace,
        )?;
        let mut total = 0usize;
        for original in self.entries.values().filter_map(Weak::upgrade) {
            if original.released.get() && original.responses.get() {
                continue;
            }
            let s = &original.snapshot;
            let matches = match self.request {
                QueryRequest::PacketRoute(q) => s.route_id == q.route_id,
                QueryRequest::PacketRoutes(q) => {
                    q.inbound_tag
                        .as_ref()
                        .is_none_or(|tag| tag == &s.inbound_tag)
                        && q.outbound_tag
                            .as_ref()
                            .is_none_or(|tag| tag == &s.outbound_tag)
                        && q.endpoint_id
                            .as_ref()
                            .is_none_or(|id| s.endpoints.iter().any(|e| &e.endpoint_id == id))
                }
                _ => unreachable!("selected packet route owner"),
            };
            if !matches {
                continue;
            }
            if total >= offset && rows.len() < limit {
                let closing = original.close.get();
                rows.push(Row { original, closing });
            }
            total += 1;
        }
        let end = offset.saturating_add(rows.len());
        Ok(PacketQueryRows {
            rows,
            core_instance_id: self.engine.core_instance_id(),
            config_revision: self.engine.config_revision(),
            observed_at_unix_ms: super::super::started_at_unix_ms(),
            total,
            next_offset: (end < total).then_some(end),
            single,
        })
    }
}
impl PacketQueryRows<'_> {
    pub(crate) fn missing(&self) -> bool {
        self.single && self.rows.is_empty()
    }
}
#[derive(Serialize)]
struct Snapshot<'a> {
    route_id: &'a str,
    core_instance_id: &'a str,
    config_revision: u64,
    inbound_tag: &'a str,
    outbound_tag: &'a str,
    endpoints: &'a [PacketRouteEndpointRef],
    source: &'a str,
    destination: &'a str,
    ip_protocol: u8,
    translated: bool,
    started_at_unix_ms: u64,
    state: PacketRouteState,
}
impl Serialize for Row {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let r = &self.original.snapshot;
        Snapshot {
            route_id: &r.route_id,
            core_instance_id: &r.core_instance_id,
            config_revision: r.config_revision,
            inbound_tag: &r.inbound_tag,
            outbound_tag: &r.outbound_tag,
            endpoints: &r.endpoints,
            source: &r.source,
            destination: &r.destination,
            ip_protocol: r.ip_protocol,
            translated: r.translated,
            started_at_unix_ms: r.started_at_unix_ms,
            state: if self.closing {
                PacketRouteState::Closing
            } else {
                PacketRouteState::Active
            },
        }
        .serialize(s)
    }
}
struct Rows<'a>(&'a [Row]);
impl Serialize for Rows<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut rows = s.serialize_seq(Some(self.0.len()))?;
        for row in self.0 {
            rows.serialize_element(row)?;
        }
        rows.end()
    }
}
impl Serialize for PacketQueryRows<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        if self.single {
            return self
                .rows
                .first()
                .expect("selected same original route")
                .serialize(s);
        }
        use serde::ser::SerializeStruct;
        let mut out = s.serialize_struct("PacketRouteListSnapshot", 6)?;
        out.serialize_field("core_instance_id", self.core_instance_id)?;
        out.serialize_field("config_revision", &self.config_revision)?;
        out.serialize_field("observed_at_unix_ms", &self.observed_at_unix_ms)?;
        out.serialize_field("routes", &Rows(&self.rows))?;
        out.serialize_field("total", &self.total)?;
        out.serialize_field("next_offset", &self.next_offset)?;
        out.end()
    }
}
#[cfg(test)]
mod tests;

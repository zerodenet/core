//! Concrete TrafficStat/TrafficStats projection from their actual owners.
pub use crate::observability::traffic::TrafficQueryError;
use crate::{
    observability::traffic::{TrafficQueryGuard, TrafficQueryRows},
    session::SessionQueryGuard,
    Engine,
};
use serde::{ser::SerializeMap, Serialize, Serializer};
use std::io;
use zero_api::{QueryRequest, TrafficScope};
use zero_runtime::configuration::ControlBudgetPermit;
/// All original guards remain synchronous; never hold this across await/refund.
/// management -> traffic membership -> active membership matches reset/publish.
pub struct EngineTrafficQueryGuard<'a> {
    engine: &'a Engine,
    // Field drop order releases inner membership before its outer management.
    active: Option<SessionQueryGuard<'a>>,
    traffic: TrafficQueryGuard<'a>,
    key: &'static str,
}
pub struct EngineTrafficQuery<'a> {
    rows: TrafficQueryRows<'a>,
    key: &'static str,
}
impl std::fmt::Debug for EngineTrafficQueryGuard<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineTrafficQueryGuard")
            .finish_non_exhaustive()
    }
}
impl std::fmt::Debug for EngineTrafficQuery<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineTrafficQuery").finish_non_exhaustive()
    }
}
impl Engine {
    pub fn control_traffic_query<'a>(
        &'a self,
        request: &'a QueryRequest,
    ) -> Option<Result<EngineTrafficQueryGuard<'a>, TrafficQueryError>> {
        let key = match request {
            QueryRequest::TrafficStat(_) => "traffic_stat",
            QueryRequest::TrafficStats(_) => "traffic_stats",
            _ => return None,
        };
        let (traffic, active) = self.traffic_query_roots();
        Some(
            traffic
                .control_query(request, self.core_instance_id())
                .map(|traffic| {
                    let active = traffic.needs_activity().then(|| active.query_guard());
                    EngineTrafficQueryGuard {
                        engine: self,
                        traffic,
                        active,
                        key,
                    }
                }),
        )
    }
}
impl EngineTrafficQueryGuard<'_> {
    pub fn freeze<'v>(
        &'v self,
        workspace: &mut ControlBudgetPermit,
    ) -> io::Result<EngineTrafficQuery<'v>> {
        let mut rows = self
            .traffic
            .freeze(self.engine.core_instance_id(), workspace)?;
        if let Some(active) = self
            .active
            .as_ref()
            .filter(|_| rows.rows().next().is_some())
        {
            let active = active.traffic_rows(workspace)?;
            // Original immutable config fragments, not endpoint_bindings deep copies.
            let config = self.traffic.configuration();
            let relevant = || {
                config.borrowed_endpoint_roles().filter(|role|rows.rows().any(|row|matches!(row.scope(),TrafficScope::Endpoint{endpoint_id} if role.matches_endpoint(endpoint_id))))
            };
            let mut roles = crate::runtime_query::workspace::vector::<
                zero_config::EndpointRoleBindingRef<'_>,
            >(relevant().count(), workspace)?;
            roles.extend(relevant());
            for row in rows.rows_mut() {
                if !matches!(
                    row.scope(),
                    TrafficScope::Peer { .. } | TrafficScope::HostInterface { .. }
                ) {
                    let (streams, datagrams) = active.activity(row.scope(), &roles);
                    row.activity.active_stream_flows = Some(streams);
                    row.activity.active_datagram_flows = Some(datagrams);
                }
            }
        }
        Ok(EngineTrafficQuery {
            rows,
            key: self.key,
        })
    }
}
impl EngineTrafficQuery<'_> {
    pub fn body(&self) -> impl Serialize + '_ {
        &self.rows
    }
}
impl Serialize for EngineTrafficQuery<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut out = s.serialize_map(Some(1))?;
        out.serialize_entry(self.key, &self.rows)?;
        out.end()
    }
}
#[cfg(test)]
mod tests;

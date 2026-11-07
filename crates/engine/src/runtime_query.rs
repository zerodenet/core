//! Bounded synchronous projection from actual Engine owners, before DTO copies.
use crate::{
    observability::{StatsQueryGuard, StatsQueryView},
    session::{ActiveQueryRows, CompletedQueryGuard, SessionQueryGuard},
    Engine, EngineRuntimeSnapshot,
};
use serde::{ser::SerializeMap, Serialize, Serializer};
use std::{io, sync::Arc};
use zero_api::QueryRequest;
use zero_runtime::configuration::ControlBudgetPermit;
mod address;
mod body;
mod legacy;
mod record;
pub(crate) mod row;
pub(crate) mod workspace;
#[derive(Clone, Copy)]
enum Kind<'a> {
    Runtime,
    Stats,
    Diagnostics,
    Active(&'a zero_api::FlowListQuery),
    Recent(&'a zero_api::FlowListQuery),
    Flow { raw: &'a str, id: Option<u64> },
}
/// Holds only original owner guards and the same immutable config generation.
/// Measure/reserve/encode synchronously, then destroy this before any await or
/// response/workspace permit refund (refund may wake another Engine caller).
pub struct EngineRuntimeQueryGuard<'a> {
    engine: &'a Engine,
    snapshot: Arc<EngineRuntimeSnapshot>,
    kind: Kind<'a>,
    active: Option<SessionQueryGuard<'a>>,
    completed: Option<CompletedQueryGuard<'a>>,
    stats: Option<StatsQueryGuard<'a>>,
}
/// Numeric rows are frozen once after every actual heap workspace is prepaid.
pub struct EngineRuntimeQuery<'a> {
    guard: &'a EngineRuntimeQueryGuard<'a>,
    active: Option<ActiveQueryRows<'a>>,
    stats: Option<StatsQueryView<'a>>,
}
#[derive(Debug)]
pub struct MissingFlow<'a>(&'a str);
impl MissingFlow<'_> {
    pub fn message_len(&self) -> Option<usize> {
        self.0.len().checked_add("flow `` was not found".len())
    }
    pub fn message(&self) -> impl std::fmt::Display + '_ {
        FlowMessage(self.0)
    }
    pub fn into_api_error(self) -> zero_api::ApiError {
        use std::fmt::Write;
        let mut text = String::with_capacity(self.message_len().expect("prepaid flow error"));
        write!(&mut text, "{}", self.message()).expect("String formatting is infallible");
        zero_api::ApiError::new(zero_api::ApiErrorCode::NotFound, text)
    }
}
struct FlowMessage<'a>(&'a str);
impl std::fmt::Display for FlowMessage<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "flow `{}` was not found", self.0)
    }
}
impl std::fmt::Debug for EngineRuntimeQueryGuard<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineRuntimeQueryGuard")
            .finish_non_exhaustive()
    }
}
impl std::fmt::Debug for EngineRuntimeQuery<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineRuntimeQuery").finish_non_exhaustive()
    }
}
impl Engine {
    pub fn control_runtime_query<'a>(
        &'a self,
        request: &'a QueryRequest,
    ) -> Option<EngineRuntimeQueryGuard<'a>> {
        let kind = match request {
            QueryRequest::Runtime(_) => Kind::Runtime,
            QueryRequest::Stats(_) => Kind::Stats,
            QueryRequest::Diagnostics(_) => Kind::Diagnostics,
            QueryRequest::ActiveFlows(q) => Kind::Active(q),
            QueryRequest::RecentFlows(q) => Kind::Recent(q),
            QueryRequest::Flow(q) => Kind::Flow {
                raw: &q.flow_id,
                id: canonical_id(&q.flow_id),
            },
            _ => return None,
        };
        let snapshot = self.runtime_snapshot();
        let (registry, history, stats) = self.query_data_roots();
        // No reverse edges: removal drops registry before finish/row work; completion
        // drops stats before history. No row mutator acquires these membership guards.
        let active = matches!(kind, Kind::Runtime | Kind::Active(_) | Kind::Flow { .. })
            .then(|| registry.query_guard());
        let completed = matches!(kind, Kind::Runtime | Kind::Recent(_) | Kind::Flow { .. })
            .then(|| history.query_guard());
        let stats = matches!(kind, Kind::Runtime | Kind::Stats | Kind::Diagnostics)
            .then(|| stats.query_guard(!matches!(kind, Kind::Diagnostics)));
        Some(EngineRuntimeQueryGuard {
            engine: self,
            snapshot,
            kind,
            active,
            completed,
            stats,
        })
    }
}
fn canonical_id(raw: &str) -> Option<u64> {
    if raw.is_empty()
        || raw.len() > 1 && raw.starts_with('0')
        || !raw.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    raw.parse().ok()
}
impl EngineRuntimeQueryGuard<'_> {
    pub fn freeze<'v>(
        &'v self,
        workspace: &mut ControlBudgetPermit,
    ) -> io::Result<Result<EngineRuntimeQuery<'v>, MissingFlow<'v>>> {
        let (filter, limit, id) = match self.kind {
            Kind::Active(q) => (Some(&q.filter), q.limit.unwrap_or(usize::MAX), None),
            Kind::Flow { raw, id } => {
                let found = id.is_some_and(|id| {
                    self.active.as_ref().is_some_and(|r| r.contains(id))
                        || self
                            .completed
                            .as_ref()
                            .is_some_and(|h| h.iter(None, 1, Some(id)).next().is_some())
                });
                if !found {
                    return Ok(Err(MissingFlow(raw)));
                }
                (None, 1, id)
            }
            _ => (None, usize::MAX, None),
        };
        let active = self
            .active
            .as_ref()
            .map(|r| r.freeze(filter, limit, id, workspace))
            .transpose()?;
        let stats = self
            .stats
            .as_ref()
            .map(|r| r.freeze(workspace))
            .transpose()?;
        Ok(Ok(EngineRuntimeQuery {
            guard: self,
            active,
            stats,
        }))
    }
}
impl EngineRuntimeQuery<'_> {
    pub fn body(&self) -> impl Serialize + '_ {
        body::Body(self)
    }
}
impl Serialize for EngineRuntimeQuery<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut map = s.serialize_map(Some(1))?;
        let key = match self.guard.kind {
            Kind::Runtime => "runtime",
            Kind::Stats => "stats",
            Kind::Diagnostics => "diagnostics",
            Kind::Active(_) => "active_flows",
            Kind::Recent(_) => "recent_flows",
            Kind::Flow { .. } => "flow",
        };
        map.serialize_entry(key, &self.body())?;
        map.end()
    }
}

#[cfg(test)]
mod tests;

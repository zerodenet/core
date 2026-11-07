//! One immutable intent/configuration and the original actual fact/meter books.
use super::*;
use crate::{
    observability::traffic::{EndpointTrafficGuard, EndpointTrafficRow},
    session::SessionQueryGuard,
};
use serde::{Serialize, Serializer};
use std::{io, sync::Arc};
use zero_api::{
    EndpointCounters, EndpointDirections, EndpointHealthState, EndpointRuntimeState, QueryRequest,
};
use zero_runtime::configuration::{ControlBudgetPermit, SourceWriteView};
mod wire;
#[derive(Clone, Copy)]
enum Selection<'a> {
    List(&'a EndpointListQuery),
    One(&'a str),
}
#[derive(Debug, Clone, Copy)]
pub struct EndpointQueryError<'a> {
    missing: &'a str,
}
impl EndpointQueryError<'_> {
    pub fn code(&self) -> zero_api::ApiErrorCode {
        zero_api::ApiErrorCode::NotFound
    }
    pub fn message(&self) -> impl std::fmt::Display + '_ {
        Missing(self.missing)
    }
}
struct Missing<'a>(&'a str);
impl std::fmt::Display for Missing<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "endpoint `{}` was not found", self.0)
    }
}
/// Pure observed data, not a provider, lifecycle callback or I/O permission.
#[derive(Debug, Clone, Copy)]
pub struct EndpointControlObservation {
    pub state: EndpointRuntimeState,
    pub health: EndpointHealthState,
    pub generation: Option<u64>,
    pub control: bool,
    pub live_contraction: EndpointDirections,
}
pub struct EngineEndpointQueryGuard<'a> {
    engine: &'a Engine,
    snapshot: Arc<crate::EngineRuntimeSnapshot>,
    selection: Selection<'a>,
    // Inner guards/rows leave before the source's outer publication guard.
    facts: facts::EndpointFactQuery<'a>,
    active: SessionQueryGuard<'a>,
    traffic: EndpointTrafficGuard<'a>,
    source: SourceWriteView<'a>,
}
pub struct EngineEndpointRows<'a> {
    rows: Vec<EngineEndpointRow<'a>>,
    list: bool,
    total: usize,
    next: Option<usize>,
}
pub struct EngineEndpointRow<'a> {
    pub(super) intent: &'a intent::Intent,
    pub(super) fact: Option<&'a Fact>,
    pub(super) traffic: Option<EndpointTrafficRow<'a>>,
    pub(super) counters: EndpointCounters,
    pub(super) core: &'a str,
    pub(super) revision: u64,
    pub(super) observed: u64,
    pub(super) source: &'a SourceWriteView<'a>,
    pub(super) path: bool,
}
impl Engine {
    /// Borrow the actual base for list/get, or the same base for Proxy's
    /// protocol detail projection. Engine alone publishes no protocol detail.
    /// Freeze/measure/encode synchronously and drop guards before await/refund.
    pub fn control_endpoint_query<'a>(
        &'a self,
        request: &'a QueryRequest,
    ) -> Option<Result<EngineEndpointQueryGuard<'a>, EndpointQueryError<'a>>> {
        let selection = match request {
            QueryRequest::Endpoints(q) => Selection::List(q),
            QueryRequest::Endpoint(q) | QueryRequest::EndpointDetails(q) => {
                Selection::One(&q.endpoint_id)
            }
            _ => return None,
        };
        let snapshot = self.runtime_snapshot();
        if let Selection::One(id) = selection {
            if !snapshot.endpoint_intents.entries.contains_key(id) {
                return Some(Err(EndpointQueryError { missing: id }));
            }
        }
        let source = self.config_source.read_write_observation();
        let traffic = self.traffic.endpoint_query();
        let active = self.session_registry.query_guard();
        let facts = self.endpoint_facts.query();
        Some(Ok(EngineEndpointQueryGuard {
            engine: self,
            snapshot,
            selection,
            facts,
            active,
            traffic,
            source,
        }))
    }
}
impl EngineEndpointQueryGuard<'_> {
    pub fn snapshot(&self) -> &crate::EngineRuntimeSnapshot {
        &self.snapshot
    }
    fn selected(&self) -> impl Iterator<Item = &intent::Intent> {
        self.snapshot
            .endpoint_intents
            .entries
            .values()
            .enumerate()
            .filter_map(move |(index, intent)| {
                let selected = match self.selection {
                    Selection::List(q) => {
                        index >= q.offset
                            && index - q.offset < q.limit.unwrap_or(100).clamp(1, 1000)
                    }
                    Selection::One(id) => intent.binding.endpoint_id == id,
                };
                selected.then_some(intent)
            })
    }
    pub fn bindings(&self) -> impl Iterator<Item = &zero_config::EndpointBindingConfig> {
        self.selected().map(|entry| &entry.binding)
    }
    pub fn freeze<'a>(
        &'a self,
        workspace: &mut ControlBudgetPermit,
    ) -> io::Result<EngineEndpointRows<'a>> {
        let count = self.selected().count();
        let mut rows =
            crate::runtime_query::workspace::vector::<EngineEndpointRow<'_>>(count, workspace)?;
        let active = (count != 0)
            .then(|| self.active.traffic_rows(workspace))
            .transpose()?;
        let revision = self.snapshot.config_revision();
        let observed = super::super::started_at_unix_ms();
        for intent in self.selected() {
            let binding = &intent.binding;
            let traffic = self.traffic.freeze(&binding.endpoint_id);
            let mut counters = traffic
                .as_ref()
                .map_or_else(Default::default, |row| row.counters.clone());
            let (streams, datagrams) = active
                .as_ref()
                .expect("selected rows retain actual activity")
                .endpoint_activity(binding);
            counters.active_stream_flows = Some(streams);
            counters.active_datagram_flows = Some(datagrams);
            rows.push(EngineEndpointRow {
                intent,
                fact: self.facts.get(&binding.endpoint_id),
                traffic,
                counters,
                core: self.engine.core_instance_id(),
                revision,
                observed,
                source: &self.source,
                path: self.engine.config_path().is_some(),
            });
        }
        let (list, total, next) = match self.selection {
            Selection::List(q) => {
                let total = self.snapshot.endpoint_intents.entries.len();
                let end = q.offset.saturating_add(rows.len());
                (true, total, (end < total).then_some(end))
            }
            Selection::One(_) => (false, 1, None),
        };
        Ok(EngineEndpointRows {
            rows,
            list,
            total,
            next,
        })
    }
}
impl EngineEndpointRows<'_> {
    pub fn rows(&self) -> impl Iterator<Item = &EngineEndpointRow<'_>> {
        self.rows.iter()
    }
    pub fn is_list(&self) -> bool {
        self.list
    }
    pub fn total(&self) -> usize {
        self.total
    }
    pub fn next_offset(&self) -> Option<usize> {
        self.next
    }
}
impl EngineEndpointRow<'_> {
    pub fn binding(&self) -> &zero_config::EndpointBindingConfig {
        &self.intent.binding
    }
    pub fn core_instance_id(&self) -> &str {
        self.core
    }
    pub fn config_revision(&self) -> u64 {
        self.revision
    }
    pub fn observed_at_unix_ms(&self) -> u64 {
        self.observed
    }
    pub fn generation(&self) -> Option<u64> {
        self.fact
            .and_then(|fact| (fact.generation > 0).then_some(fact.generation))
    }
    pub fn observed(&self, observation: EndpointControlObservation) -> impl Serialize + '_ {
        wire::Observed {
            row: self,
            observation: Some(observation),
        }
    }
}
impl Serialize for EngineEndpointRow<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        wire::Observed {
            row: self,
            observation: None,
        }
        .serialize(s)
    }
}
impl std::fmt::Debug for EngineEndpointQueryGuard<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineEndpointQueryGuard")
            .finish_non_exhaustive()
    }
}

impl Serialize for EngineEndpointRows<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut out = s.serialize_map(Some(1))?;
        out.serialize_entry(
            if self.list { "endpoints" } else { "endpoint" },
            &wire::Body(self),
        )?;
        out.end()
    }
}
impl EngineEndpointRows<'_> {
    pub fn body(&self) -> impl Serialize + '_ {
        wire::Body(self)
    }
}

#[cfg(test)]
mod tests;

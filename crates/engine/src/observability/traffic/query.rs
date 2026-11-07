//! Membership/period borrowing with prepaid exact row workspace; no DTO copies.
use super::{
    counter::{now, CounterSet, Period},
    TrafficRegistry,
};
use crate::runtime_query::workspace;
use std::{
    collections::BTreeMap,
    io,
    sync::{atomic::Ordering, Arc, MutexGuard, RwLockReadGuard},
};
use zero_api::{QueryRequest, TrafficActivity, TrafficGetQuery, TrafficListQuery, TrafficScope};
use zero_runtime::configuration::ControlBudgetPermit;
mod row;
use row::FrozenPlane;
mod wire;
#[derive(Clone, Copy)]
enum Kind<'a> {
    One(&'a TrafficGetQuery),
    List(&'a TrafficListQuery),
}
#[derive(Clone, Copy, Debug)]
pub struct TrafficQueryError {
    code: zero_api::ApiErrorCode,
    message: &'static str,
}
impl TrafficQueryError {
    pub fn code(&self) -> zero_api::ApiErrorCode {
        self.code
    }
    pub fn message(&self) -> &'static str {
        self.message
    }
    fn new(code: zero_api::ApiErrorCode, message: &'static str) -> Self {
        Self { code, message }
    }
}
pub(crate) struct TrafficQueryGuard<'a> {
    registry: &'a TrafficRegistry,
    configuration: Arc<zero_config::RuntimeConfig>,
    kind: Kind<'a>,
    entries: RwLockReadGuard<'a, BTreeMap<TrafficScope, Arc<CounterSet>>>,
    _management: MutexGuard<'a, ()>,
}
pub(crate) struct TrafficQueryRows<'a> {
    core: &'a str,
    revision: u64,
    registry_revision: u64,
    sampled: u64,
    list: bool,
    total: usize,
    next: Option<usize>,
    rows: Vec<TrafficRow<'a>>,
}
pub(crate) struct TrafficRow<'a> {
    scope: &'a TrafficScope,
    period: MutexGuard<'a, Period>,
    generation: Option<u64>,
    started: u64,
    monotonic: u64,
    sampled: u64,
    source_time: Option<u64>,
    source_monotonic: Option<u64>,
    planes: [FrozenPlane; 4],
    pub(crate) activity: TrafficActivity,
}
impl TrafficRow<'_> {
    pub(crate) fn scope(&self) -> &TrafficScope {
        self.scope
    }
}
impl TrafficRegistry {
    pub(crate) fn control_query<'a>(
        &'a self,
        request: &'a QueryRequest,
        core: &str,
    ) -> Option<Result<TrafficQueryGuard<'a>, TrafficQueryError>> {
        let kind = match request {
            QueryRequest::TrafficStat(q) => Kind::One(q),
            QueryRequest::TrafficStats(q) => Kind::List(q),
            _ => return None,
        };
        Some(self.control_query_kind(kind, core))
    }
    fn control_query_kind<'a>(
        &'a self,
        kind: Kind<'a>,
        core: &str,
    ) -> Result<TrafficQueryGuard<'a>, TrafficQueryError> {
        use zero_api::ApiErrorCode as Code;
        if matches!(kind,Kind::List(q) if q.scopes.len()>256) {
            return Err(TrafficQueryError::new(
                Code::InvalidArgument,
                "at most 256 selected scopes",
            ));
        }
        let management = self.management.lock().unwrap_or_else(|e| e.into_inner());
        let revision = self.config_revision.load(Ordering::Relaxed);
        if matches!(kind,Kind::List(q) if q.expected_core_instance_id.as_deref().is_some_and(|v|v!=core)||q.expected_config_revision.is_some_and(|v|v!=revision))
        {
            return Err(TrafficQueryError::new(
                Code::Conflict,
                "statistics query instance or configuration changed",
            ));
        }
        let entries = self.entries.read().unwrap_or_else(|e| e.into_inner());
        if matches!(kind,Kind::List(q) if q.expected_registry_revision.is_some_and(|v|v!=self.revision.load(Ordering::Relaxed)))
        {
            return Err(TrafficQueryError::new(
                Code::Conflict,
                "statistics registry changed; restart pagination",
            ));
        }
        match kind {
            Kind::One(q) if !entries.contains_key(&q.scope) => {
                return Err(TrafficQueryError::new(
                    Code::NotFound,
                    "statistics scope no longer exists",
                ))
            }
            Kind::List(q) if q.scopes.iter().any(|s| !entries.contains_key(s)) => {
                return Err(TrafficQueryError::new(
                    Code::NotFound,
                    "selected statistics scope no longer exists",
                ))
            }
            _ => {}
        }
        Ok(TrafficQueryGuard {
            registry: self,
            configuration: self
                .configuration
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .as_ref()
                .expect("same Engine initialized the actual traffic projection")
                .0
                .clone(),
            kind,
            _management: management,
            entries,
        })
    }
}
impl TrafficQueryGuard<'_> {
    pub(crate) fn configuration(&self) -> &zero_config::RuntimeConfig {
        &self.configuration
    }

    pub(crate) fn needs_activity(&self) -> bool {
        match self.kind {
            Kind::One(q) => !matches!(
                q.scope,
                TrafficScope::Peer { .. } | TrafficScope::HostInterface { .. }
            ),
            Kind::List(q) => {
                q.scopes.is_empty()
                    || q.scopes.iter().any(|s| {
                        !matches!(
                            s,
                            TrafficScope::Peer { .. } | TrafficScope::HostInterface { .. }
                        )
                    })
            }
        }
    }
    pub(crate) fn freeze<'q>(
        &'q self,
        core: &'q str,
        workspace: &mut ControlBudgetPermit,
    ) -> io::Result<TrafficQueryRows<'q>> {
        let mut total = 1;
        let mut offset = 0;
        let mut list = false;
        let selected = match self.kind {
            Kind::List(q) if !q.scopes.is_empty() => {
                let mut selected = workspace::vector::<&TrafficScope>(q.scopes.len(), workspace)?;
                selected.extend(q.scopes.iter());
                selected.sort_unstable();
                selected.dedup();
                Some(selected)
            }
            _ => None,
        };
        let count = match self.kind {
            Kind::One(_) => 1,
            Kind::List(q) => {
                list = true;
                offset = q.offset;
                let available = selected.as_ref().map_or_else(
                    || {
                        self.entries
                            .keys()
                            .filter(|s| {
                                q.include_host_interfaces
                                    || !matches!(s, TrafficScope::HostInterface { .. })
                            })
                            .count()
                    },
                    Vec::len,
                );
                available
                    .saturating_sub(q.offset)
                    .min(q.limit.unwrap_or(64).clamp(1, 256))
            }
        };
        let mut rows = workspace::vector::<TrafficRow<'_>>(count, workspace)?;
        match self.kind {
            Kind::One(q) => {
                let (scope, entry) = self
                    .entries
                    .get_key_value(&q.scope)
                    .expect("same retained membership");
                rows.push(freeze(scope, entry));
            }
            Kind::List(q) => {
                let limit = q.limit.unwrap_or(64).clamp(1, 256);
                if q.scopes.is_empty() {
                    let selected = self.entries.iter().filter(|(s, _)| {
                        q.include_host_interfaces
                            || !matches!(s, TrafficScope::HostInterface { .. })
                    });
                    total = selected.clone().count();
                    for (scope, entry) in selected.skip(q.offset).take(limit) {
                        rows.push(freeze(scope, entry));
                    }
                } else {
                    // Same original selected identities, sorted/deduplicated without allocating
                    // a BTreeSet or scanning unrelated endpoint/peer inventories.
                    let selected = selected
                        .expect("same selected identities were prepaid before row allocation");
                    total = selected.len();
                    for scope in selected.into_iter().skip(q.offset).take(limit) {
                        let (scope, entry) = self
                            .entries
                            .get_key_value(scope)
                            .expect("same retained membership");
                        rows.push(freeze(scope, entry));
                    }
                }
            }
        }
        let end = offset.saturating_add(rows.len());
        Ok(TrafficQueryRows {
            core,
            revision: self.registry.config_revision.load(Ordering::Relaxed),
            registry_revision: self.registry.revision.load(Ordering::Relaxed),
            sampled: now(),
            list,
            total,
            next: (end < total).then_some(end),
            rows,
        })
    }
}
impl<'a> TrafficQueryRows<'a> {
    pub(crate) fn rows(&self) -> impl Iterator<Item = &TrafficRow<'a>> {
        self.rows.iter()
    }
    pub(crate) fn rows_mut(&mut self) -> impl Iterator<Item = &mut TrafficRow<'a>> {
        self.rows.iter_mut()
    }
}
fn freeze<'a>(scope: &'a TrafficScope, entry: &'a CounterSet) -> TrafficRow<'a> {
    let period = entry.period.lock().unwrap_or_else(|e| e.into_inner());
    let started = now();
    let values = entry.capture();
    let planes = std::array::from_fn(|i| {
        FrozenPlane::new(i, &entry.planes[i], values[i], period.baseline[i], scope)
    });
    let present = |value| if value == 0 { None } else { Some(value) };
    TrafficRow {
        scope,
        period,
        generation: *entry.generation.lock().unwrap_or_else(|e| e.into_inner()),
        started,
        monotonic: entry.clock.elapsed().as_nanos().min(u64::MAX as u128) as u64,
        sampled: now(),
        source_time: present(entry.source_sampled_at.load(Ordering::Relaxed)),
        source_monotonic: present(entry.source_sampled_monotonic.load(Ordering::Relaxed)),
        planes,
        activity: TrafficActivity {
            active_stream_flows: (entry.flows_available.load(Ordering::Acquire) != 0)
                .then(|| entry.active_streams.load(Ordering::Relaxed)),
            active_datagram_flows: (entry.flows_available.load(Ordering::Acquire) != 0)
                .then(|| entry.active_datagrams.load(Ordering::Relaxed)),
            active_packet_routes: (entry.routes_available.load(Ordering::Acquire) != 0)
                .then(|| entry.active_routes.load(Ordering::Relaxed)),
        },
    }
}

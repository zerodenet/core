use zero_api::{EndpointListQuery, EventFilter, EventReplay, RawApiEvent};

use super::Engine;
use crate::{ActiveSession, CompletedSessionRecord};

impl Engine {
    pub(crate) fn control_snapshot_basis(&self) -> crate::observability::SnapshotBasis<'_> {
        self.event_log.flow_snapshot_basis()
    }
    pub(crate) fn traffic_query_roots(
        &self,
    ) -> (
        &crate::observability::traffic::TrafficRegistry,
        &crate::session::SessionRegistry,
    ) {
        (&self.traffic, &self.session_registry)
    }

    pub(crate) fn query_data_roots(
        &self,
    ) -> (
        &crate::session::SessionRegistry,
        &crate::session::CompletedSessionHistory,
        &crate::observability::EngineStats,
    ) {
        (
            &self.session_registry,
            &self.completed_sessions,
            &self.stats,
        )
    }

    pub(crate) fn prepare_event_subscriber(
        &self,
        filter: EventFilter,
    ) -> std::io::Result<crate::observability::PreparedSubscriber> {
        self.event_log.prepare_subscriber(filter)
    }

    pub(crate) fn flow_snapshot_event(&self) -> RawApiEvent {
        let (sessions, session_registry_revision, principals) =
            self.session_registry.flow_snapshot();
        self.event_log
            .flow_snapshot_event(&sessions, session_registry_revision, &principals)
    }

    pub(crate) fn emit_event(&self, event: RawApiEvent) {
        self.event_log.push_external(event);
    }

    pub fn stats_snapshot(&self) -> zero_api::StatsSnapshot {
        self.stats.snapshot()
    }

    pub fn active_sessions(&self) -> Vec<ActiveSession> {
        self.session_registry.snapshot()
    }

    pub fn principal_flows_snapshot(&self) -> zero_api::PrincipalFlowsSnapshot {
        let (session_registry_revision, principals) = self.session_registry.principal_snapshot();
        zero_api::PrincipalFlowsSnapshot {
            core_instance_id: self.core_instance_id().to_owned(),
            session_registry_revision,
            principals,
        }
    }

    pub fn completed_sessions(&self) -> Vec<CompletedSessionRecord> {
        self.completed_sessions.snapshot()
    }

    pub fn events_snapshot(&self, filter: &EventFilter) -> Vec<RawApiEvent> {
        self.event_log.snapshot(filter)
    }

    pub fn events_since(&self, since: u64, limit: usize, filter: &EventFilter) -> EventReplay {
        self.event_log.events_since(since, limit, filter)
    }

    /// Prepaid original replay for concrete production control adapters. The
    /// existing owned EventSource API retains its independent library role.
    pub fn retained_event_replay(
        &self,
        since: u64,
        limit: usize,
        filter: &EventFilter,
        credit: zero_runtime::configuration::ControlBudgetPermit,
    ) -> std::io::Result<crate::RetainedReplay> {
        self.event_log.retained_replay(since, limit, filter, credit)
    }

    /// Synchronous borrowing of this Engine's original history. Consumers
    /// reserve their own custody before cloning an immutable event Arc.
    pub fn original_event_history(&self) -> crate::OriginalEventHistory<'_> {
        self.event_log.original_history()
    }

    pub fn latest_event_sequence(&self) -> u64 {
        self.event_log.latest_sequence()
    }

    pub fn push_stats_sampled(&self) {
        self.event_log.push_stats_sampled(&self.stats_snapshot());
        self.push_traffic_stats_sampled();
    }

    pub fn push_endpoint_stats_sampled_page(&self, offset: usize) -> usize {
        let page = self.endpoints_snapshot(&EndpointListQuery {
            offset,
            limit: Some(64),
        });
        self.event_log.push_endpoint_stats_sampled(&page.endpoints);
        page.next_offset.unwrap_or(0)
    }

    pub fn push_endpoint_stats_sampled(&self) {
        let snapshot = self.runtime_snapshot();
        let mut offset = 0;
        loop {
            let page = self.endpoints_snapshot_in(
                &snapshot,
                &EndpointListQuery {
                    offset,
                    limit: Some(64),
                },
            );
            if page.endpoints.is_empty() {
                break;
            }
            self.event_log.push_endpoint_stats_sampled(&page.endpoints);
            let Some(next) = page.next_offset else {
                break;
            };
            offset = next;
        }
    }

    pub fn push_engine_stopped(&self, reason: &str) {
        self.event_log.push_engine_stopped(reason);
    }

    pub fn update_sink_status(&self, status: Vec<zero_api::SinkStatus>) {
        *self.sink_status.lock().expect("sink status lock poisoned") = status;
    }

    pub(crate) fn query_sink_status(&self) -> std::sync::MutexGuard<'_, Vec<zero_api::SinkStatus>> {
        self.sink_status.lock().expect("sink status lock poisoned")
    }
    pub fn sink_status_snapshot(&self) -> zero_api::SinkStatusSnapshot {
        zero_api::SinkStatusSnapshot {
            sinks: self
                .sink_status
                .lock()
                .expect("sink status lock poisoned")
                .clone(),
        }
    }
}

#[cfg(test)]
impl Engine {
    pub(crate) fn test_subscribe_events(
        &self,
        tx: std::sync::mpsc::SyncSender<crate::observability::QueuedEvent>,
        filter: EventFilter,
        slots: usize,
    ) -> std::io::Result<(
        crate::observability::SubscriberRegistration,
        std::sync::Arc<std::sync::atomic::AtomicUsize>,
    )> {
        self.event_log.test_subscriber(tx, filter, slots)
    }
}

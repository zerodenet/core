use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use zero_core::{Address, Session, SessionAuth};

use super::Engine;
use crate::observability::SessionOutcome;
use crate::session::{
    CompletedSessionRecord, FlowContext, FlowHook, FlowTraffic, SessionHandle, SessionTrafficUpdate,
};
use crate::{
    EngineError, FlowFailureObservation, FlowRemoteEndpoint, FlowRouteObservation, RouteDecision,
    RouteTrace,
};

impl Engine {
    pub(crate) fn register_session_cancellation<F>(&self, id: u64, cancel: F) -> bool
    where
        F: FnOnce() + Send + 'static,
    {
        self.session_registry.register_cancellation(id, cancel)
    }

    pub(crate) fn session_is_cancelled(&self, id: u64) -> bool {
        self.session_registry.is_cancelled(id)
    }

    pub(crate) fn session_cancellation_reason(&self, id: u64) -> Option<String> {
        self.session_registry.cancellation_reason(id)
    }

    pub fn prepare_session(
        &self,
        session: &mut Session,
        inbound_tag: &str,
    ) -> Result<(), EngineError> {
        session.id = self.next_session_id.fetch_add(1, Ordering::Relaxed);
        session.inbound_tag = Some(inbound_tag.to_owned());
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let mode = self.mode.lock().unwrap_or_else(|error| error.into_inner());
        self.principal_policies.validate(session.auth.as_ref())?;
        let hook = self
            .flow_hook
            .read()
            .expect("flow hook lock poisoned")
            .clone();
        if let Some(hook) = hook {
            let context = FlowContext::from_session(session, mode.kind(), now_ms);
            if let Err(reason) = hook.on_flow_start(&context) {
                tracing::warn!(flow_id = session.id, reason = %reason.message, "flow blocked by hook");
                return Err(EngineError::AdmissionDenied {
                    reason: reason.message,
                });
            }
        }
        let device_registration =
            self.acquire_principal_device(session.auth.as_ref(), session.source_ip.as_ref())?;
        let quota_registration = self.principal_quotas.acquire(session.auth.as_ref())?;
        let inserted = self.session_registry.insert(
            session,
            mode.kind(),
            device_registration,
            quota_registration,
        );
        self.stats.record_start();
        self.event_log
            .push_flow_started(&inserted.active, inserted.principal_observation.as_ref());
        Ok(())
    }

    pub fn acquire_principal_device(
        &self,
        auth: Option<&SessionAuth>,
        source_ip: Option<&Address>,
    ) -> Result<Option<crate::PrincipalDeviceRegistration>, EngineError> {
        self.principal_policies.validate(auth)?;
        let Some(auth) = auth else {
            return Ok(None);
        };
        let Some(limit) = auth.device_limit.filter(|limit| *limit > 0) else {
            return Ok(None);
        };
        let principal_key =
            auth.principal_key
                .as_deref()
                .ok_or_else(|| EngineError::AdmissionDenied {
                    reason: "device-limited session has no principal identity".to_owned(),
                })?;
        let source_ip = source_ip
            .cloned()
            .ok_or_else(|| EngineError::AdmissionDenied {
                reason: format!(
                    "device-limited principal `{principal_key}` has no observable source IP"
                ),
            })?;
        self.principal_devices
            .acquire(principal_key, source_ip, limit)
            .map(Some)
            .ok_or_else(|| EngineError::AdmissionDenied {
                reason: format!("principal `{principal_key}` exceeded its {limit}-device limit"),
            })
    }

    pub fn set_session_outbound(&self, session: &Session) {
        self.set_session_outbound_with_path(session, None, Vec::new());
    }

    pub fn set_session_outbound_with_remote(&self, session: &Session, remote: Option<(&str, u16)>) {
        self.set_session_outbound_with_path(session, remote, Vec::new());
    }

    pub fn set_session_outbound_with_path(
        &self,
        session: &Session,
        remote: Option<(&str, u16)>,
        relay_chain: Vec<(String, String)>,
    ) {
        let outbound_protocol = session
            .outbound_tag
            .as_deref()
            .and_then(|tag| self.outbound_protocol_for_tag(tag));
        let active = self.session_registry.update_outbound(
            session.id,
            session.outbound_tag.as_deref(),
            outbound_protocol,
            remote.map(|(host, port)| FlowRemoteEndpoint {
                host: host.to_owned(),
                port,
            }),
            relay_chain,
        );
        if let Some(active) = active {
            self.event_log.push_flow_routed(&active);
        }
    }

    pub fn record_session_route(&self, id: u64, trace: &RouteTrace) {
        let (action, target) = match &trace.decision {
            RouteDecision::Route(tag) => ("route".to_owned(), Some(tag.clone())),
            RouteDecision::Direct => ("direct".to_owned(), None),
            RouteDecision::Reject => ("reject".to_owned(), None),
        };
        let selection_chain = target.iter().cloned().collect();
        self.session_registry.update_route(
            id,
            FlowRouteObservation {
                mode: trace.mode.clone(),
                action,
                target,
                matched_rule: trace.matched_rule.clone(),
                selection_chain,
            },
        );
    }

    pub fn record_session_upload(&self, id: u64, bytes: u64) {
        self.record_session_traffic(self.session_registry.record_upload(id, bytes));
    }
    pub fn record_session_download(&self, id: u64, bytes: u64) {
        self.record_session_traffic(self.session_registry.record_download(id, bytes));
    }
    pub fn record_session_inbound_rx(&self, id: u64, bytes: u64) {
        self.record_session_traffic(self.session_registry.record_inbound_rx(id, bytes));
    }
    pub fn record_session_inbound_tx(&self, id: u64, bytes: u64) {
        self.record_session_traffic(self.session_registry.record_inbound_tx(id, bytes));
    }
    pub fn record_session_outbound_rx(&self, id: u64, bytes: u64) {
        self.record_session_traffic(self.session_registry.record_outbound_rx(id, bytes));
    }
    pub fn record_session_outbound_tx(&self, id: u64, bytes: u64) {
        self.record_session_traffic(self.session_registry.record_outbound_tx(id, bytes));
    }
    pub fn record_udp_upstream_association_created(&self) {
        self.stats.record_udp_upstream_association_created();
    }
    pub fn record_udp_upstream_association_reused(&self) {
        self.stats.record_udp_upstream_association_reused();
    }
    pub fn record_udp_upstream_association_closed(&self) {
        self.stats.record_udp_upstream_association_closed();
    }
    pub fn record_udp_upstream_association_idle_timeout(&self) {
        self.stats.record_udp_upstream_association_idle_timeout();
    }
    pub fn record_udp_upstream_association_dropped(&self) {
        self.stats.record_udp_upstream_association_dropped();
    }
    pub fn record_udp_upstream_association_failed(&self) {
        self.stats.record_udp_upstream_association_failed();
    }
    pub fn record_udp_upstream_send_failure(&self) {
        self.stats.record_udp_upstream_send_failure();
    }
    pub fn record_udp_upstream_recv_failure(&self) {
        self.stats.record_udp_upstream_recv_failure();
    }
    pub fn record_udp_upstream_packet_sent(&self) {
        self.stats.record_udp_upstream_packet_sent();
    }
    pub fn record_udp_upstream_packet_received(&self) {
        self.stats.record_udp_upstream_packet_received();
    }

    pub fn finish_session(
        &self,
        id: u64,
        outcome: SessionOutcome,
    ) -> Option<CompletedSessionRecord> {
        self.finish_session_with_reason(id, outcome, None)
    }

    pub fn finish_session_with_reason(
        &self,
        id: u64,
        outcome: SessionOutcome,
        reason: Option<String>,
    ) -> Option<CompletedSessionRecord> {
        self.finish_session_with_observation(id, outcome, reason, None)
    }

    pub fn finish_session_with_observation(
        &self,
        id: u64,
        outcome: SessionOutcome,
        reason: Option<String>,
        failure: Option<FlowFailureObservation>,
    ) -> Option<CompletedSessionRecord> {
        let finished = self.session_registry.finish(id, outcome, reason, failure)?;
        let principal_observation = finished.principal_observation;
        let record = finished.record;
        self.stats.record_live_traffic(
            finished.traffic_delta.bytes_up,
            finished.traffic_delta.bytes_down,
        );
        self.stats.record_finish(outcome);
        self.stats.record_completed_outbound_traffic(
            record.outbound_tag.as_deref(),
            record.bytes_up,
            record.bytes_down,
        );
        self.completed_sessions.push(record.clone());
        let completed_event =
            self.event_log
                .prepare_flow_completed(&record, principal_observation.as_ref(), |tag| {
                    self.outbound_protocol_for_tag(tag)
                });
        let completion_sink = self
            .flow_completion_sink
            .read()
            .expect("flow completion sink lock poisoned")
            .clone();
        if let Some(sink) = completion_sink {
            match sink.0.publish(&completed_event) {
                Ok(result) if result.delivered => {}
                Ok(result) => self.emit_warning(
                    "flow_completion_persistence_failed",
                    result
                        .message
                        .as_deref()
                        .unwrap_or("flow completion sink did not confirm durable delivery"),
                ),
                Err(error) => {
                    self.emit_warning("flow_completion_persistence_failed", &error.to_string())
                }
            }
        }
        self.event_log.push_prepared_generated(completed_event);
        let hook = self
            .flow_hook
            .read()
            .expect("flow hook lock poisoned")
            .clone();
        if let Some(hook) = hook {
            hook.on_flow_end(
                &FlowContext::from_completed(&record),
                outcome,
                &FlowTraffic::from_completed(&record),
            );
        }
        Some(record)
    }

    pub fn track_session(&self, id: u64) -> SessionHandle {
        SessionHandle::new(self.clone(), id)
    }
    pub fn check_outbound_health(&self, tag: &str) -> Result<(), EngineError> {
        self.outbound_health.check(tag)
    }
    pub fn record_outbound_failure(&self, tag: &str) {
        self.outbound_health.record_failure(tag);
    }
    pub fn record_outbound_success(&self, tag: &str) {
        self.outbound_health.record_success(tag);
    }
    pub fn probe_trigger_registry(&self) -> &crate::health::ProbeTriggerRegistry {
        &self.probe_trigger_registry
    }

    pub fn trigger_urltest_probe(
        &self,
        tag: &str,
        requested_operation_id: Option<&str>,
    ) -> Result<crate::health::ProbeTriggerAck, EngineError> {
        let operation_id = self.operation_id(requested_operation_id);
        Ok(self
            .probe_trigger_registry
            .get(tag)
            .ok_or_else(|| EngineError::SelectorGroupNotFound {
                tag: tag.to_owned(),
            })?
            .trigger(operation_id))
    }

    pub fn close_flow(&self, flow_id: &str) -> Result<(), EngineError> {
        let id = flow_id.parse().map_err(|_| {
            EngineError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalid flow id",
            ))
        })?;
        if self.session_registry.cancel(id, "manual") {
            Ok(())
        } else {
            Err(EngineError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("flow `{flow_id}` not found or already completed"),
            )))
        }
    }

    pub fn close_principal_flows(&self, principal_key: &str, reason: &str) -> Vec<u64> {
        let cancelled = self
            .session_registry
            .cancel_principal(principal_key, reason);
        self.principal_cancellations.cancel(principal_key, reason);
        cancelled
    }

    pub fn forget_principal_policy_state(&self, principal_key: &str) {
        self.principal_quotas.forget(principal_key);
    }

    fn cancel_if_quota_exhausted(&self, principal_key: Option<String>) {
        if let Some(principal_key) = principal_key {
            self.close_principal_flows(&principal_key, "quota_exhausted");
        }
    }

    fn record_session_traffic(&self, update: Option<SessionTrafficUpdate>) {
        let Some(update) = update else {
            return;
        };
        self.stats
            .record_live_traffic(update.delta.bytes_up, update.delta.bytes_down);
        self.cancel_if_quota_exhausted(update.quota_exhausted_principal);
    }

    pub fn register_principal_cancellation<F>(
        &self,
        principal_key: &str,
        callback: F,
    ) -> crate::PrincipalCancellationRegistration
    where
        F: FnOnce(String) + Send + 'static,
    {
        self.principal_cancellations
            .register(principal_key, callback)
    }

    fn outbound_protocol_for_tag(&self, tag: &str) -> Option<&'static str> {
        if tag == "direct" {
            return Some("direct");
        }
        if tag == "block" {
            return Some("block");
        }
        self.config()
            .outbounds
            .iter()
            .find(|outbound| outbound.tag == tag)
            .map(|outbound| outbound.protocol.protocol_name())
    }
}

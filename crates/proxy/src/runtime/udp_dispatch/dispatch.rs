use std::time::Instant;

use zero_core::{Network, Session};
use zero_engine::{EngineError, SessionOutcome};

use super::model::UdpFlowCancellation;
use super::{FlowStartResult, UdpDispatch};
use crate::logging::{log_session_failed, log_session_finished, session_failure_observation};
use crate::runtime::passive_relay_health::classify_relay_outcome;
use crate::runtime::pipe::UdpPipeInput;
use crate::runtime::udp_flow::rate_limit::UdpFlowRateLimiters;
use crate::runtime::udp_flow::sessions::UdpFlowKey;

impl UdpDispatch {
    /// Dispatch a UDP packet: route, select outbound, send.
    ///
    /// If a flow already exists for `(target, port, client_session_id)`, forwards
    /// the payload. Otherwise creates a new session, routes through the engine,
    /// and dispatches to the resolved outbound.
    pub(crate) async fn dispatch(&mut self, input: UdpPipeInput<'_>) -> Result<u64, EngineError> {
        if let Some(flow) = self
            .flows
            .snapshot(&input.target, input.port, input.client_session_id)
        {
            self.forward_existing(&flow, input.payload).await?;
            return Ok(flow.session.id);
        }

        let key = UdpFlowKey::new(&input.target, input.port, input.client_session_id);
        if let Some(retry_after) = self.flow_start_backoff.retry_after(&key, Instant::now()) {
            return Err(EngineError::AdmissionDenied {
                reason: format!(
                    "UDP flow is cooling down after an outbound failure; retry in {} ms",
                    retry_after.as_millis()
                ),
            });
        }
        let result = self.start_new_routed_flow(input).await;
        match result {
            Ok(session_id) => {
                self.flow_start_backoff.clear(&key);
                Ok(session_id)
            }
            Err(error) => {
                self.flow_start_backoff.record_failure(key, Instant::now());
                Err(error)
            }
        }
    }

    async fn start_new_routed_flow(&mut self, input: UdpPipeInput<'_>) -> Result<u64, EngineError> {
        let runtime = self.runtime.clone();
        let ingress_key = UdpFlowKey::new(&input.target, input.port, input.client_session_id);
        let mut session = Session::new(0, input.target, input.port, Network::Udp, input.protocol);
        session.transparent_target = input.transparent_target;
        if let Some(original_target) = input.transparent_original_target {
            session.original_target = Some(original_target.clone());
            session.direct_target = Some(original_target);
            session.target_host_source = input.transparent_host_source;
            if input.transparent_host_source == Some(zero_core::TargetHostSource::QuicSni) {
                if let zero_core::Address::Domain(domain) = &session.target {
                    session.sni = Some(domain.clone());
                }
            }
        }
        if let Some(auth) = input.auth {
            session.apply_auth(auth.clone());
        }
        if let Some(source_addr) = input.source_addr {
            session.source_ip = Some(match source_addr.ip() {
                std::net::IpAddr::V4(ip) => zero_core::Address::Ipv4(ip.octets()),
                std::net::IpAddr::V6(ip) => zero_core::Address::Ipv6(ip.octets()),
            });
            session.source_port = Some(source_addr.port());
        }
        runtime.resolve_fake_ip_target(&mut session).await;
        runtime
            .prepare_udp_session(&mut session, &self.inbound_tag)
            .await?;
        let mut session_handle = runtime.track_session(session.id);
        let rate_limiters = UdpFlowRateLimiters::new(runtime.traffic_rate_limiters(&session));
        let cancellation_rate_limiters = rate_limiters.clone();
        let cancel_tx = self.cancel_tx.clone();
        let cancelled_session_id = session.id;
        let close_association = session
            .auth
            .as_ref()
            .is_some_and(|auth| auth.principal_key.is_some());
        session_handle.register_cancellation(move || {
            cancellation_rate_limiters.cancel();
            let _ = cancel_tx.send(UdpFlowCancellation::new(
                cancelled_session_id,
                close_association,
            ));
        });
        if !rate_limiters.throttle_upload(input.payload.len()).await {
            let reason = session_handle
                .cancellation_reason()
                .unwrap_or_else(|| "cancelled".to_owned());
            let _ =
                session_handle.finish_with_reason(SessionOutcome::Cancelled, Some(reason.clone()));
            return Err(EngineError::Io(std::io::Error::other(format!(
                "UDP flow cancelled while upload was rate limited: {reason}"
            ))));
        }
        let started_at = Instant::now();
        runtime
            .services()
            .record_session_inbound_rx(session.id, input.payload.len() as u64);

        let action = runtime.route_decision(&session).await;
        let (resolved, passive_relay_selections) = match runtime.resolve_outbound(&action, &session)
        {
            Ok(resolved) => resolved,
            Err(error) => {
                let record = session_handle.finish_with_failure(
                    "upstream_error",
                    session_failure_observation("resolve_outbound", &error, None),
                );
                log_session_failed(
                    &session,
                    record.as_ref(),
                    "resolve_outbound",
                    started_at.elapsed(),
                    &error,
                    None,
                );
                return Err(error);
            }
        };
        runtime.log_session_accepted(&session, &action);

        match runtime
            .start_udp_resolved_outbound(self, &session, resolved, input.payload)
            .await
        {
            Ok(FlowStartResult::Flow { outbound, tx_bytes }) => {
                let session_id = session.id;
                session.outbound_tag = Some(outbound.tag().to_owned());
                let remote = outbound.observed_remote();
                runtime.set_session_outbound(&session, Some(&remote));
                self.flows.insert(
                    ingress_key,
                    session.clone(),
                    session_handle,
                    *outbound,
                    passive_relay_selections.clone(),
                    rate_limiters,
                );
                runtime
                    .services()
                    .record_session_outbound_tx(session_id, tx_bytes);
                Ok(session_id)
            }
            Ok(FlowStartResult::Blocked { tag }) => {
                session.outbound_tag = Some(tag);
                runtime.set_session_outbound(&session, None);
                if let Some(record) = session_handle.finish(SessionOutcome::Blocked) {
                    log_session_finished(&record, None);
                }
                Ok(session.id)
            }
            Err(failure) => {
                let stage = failure.stage;
                let upstream = failure
                    .upstream
                    .as_ref()
                    .map(|(server, port)| (server.as_str(), *port));
                let record = session_handle.finish_with_failure(
                    "upstream_error",
                    session_failure_observation(stage, &failure.error, upstream),
                );
                log_session_failed(
                    &session,
                    record.as_ref(),
                    stage,
                    started_at.elapsed(),
                    &failure.error,
                    upstream,
                );
                if let Some(record) = record.as_ref() {
                    runtime.record_passive_relay_outcome(
                        &passive_relay_selections,
                        &session,
                        classify_relay_outcome(record, Some(&failure.error)),
                    );
                }
                Err(failure.error)
            }
        }
    }
}

#[cfg(test)]
mod tests;

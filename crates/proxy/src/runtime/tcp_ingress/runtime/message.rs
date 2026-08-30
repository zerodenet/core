use std::time::Instant;

use zero_engine::{EngineError, SessionOutcome};

use super::super::contract::{MessageInboundProtocol, MessageRelayContext, MessageRelayOutcome};
use super::super::lifecycle::result::{
    finish_blocked, finish_relay_failure, finish_relay_success, finish_route_or_establish_failure,
};
use super::TcpIngressRuntime;
use crate::runtime::passive_relay_health::classify_relay_outcome;
use crate::runtime::pipe::{KernelPipe, TcpPipe, TcpPipeInput};
use crate::transport::{is_block_error, TcpRelayStream};

impl TcpIngressRuntime {
    pub(crate) async fn serve_message<P>(
        &self,
        protocol: &P,
        client: &mut TcpRelayStream,
        mut request: P::Request,
    ) -> Result<MessageRelayOutcome, EngineError>
    where
        P: MessageInboundProtocol,
    {
        let original_target = protocol.session(&request).target.clone();
        let original_port = protocol.session(&request).port;
        let mut session = protocol.session(&request).clone();
        let started_at = Instant::now();
        let target_resolution = self.resolve_fake_ip_target(&mut session).await;
        if target_resolution.is_ok() {
            self.apply_url_rewrite(&mut session);
        }
        if session.target != original_target || session.port != original_port {
            protocol.set_effective_target(&mut request, &session.target, session.port);
        }
        self.apply_kernel_rate_limits(&mut session);
        self.prepare_session(&mut session).await?;
        let rate_limiters = self.traffic_rate_limiters(&session);
        let mut handle = self.track_session(session.id);
        if let Err(error) = target_resolution {
            let _ = protocol.send_upstream_failure(client).await;
            crate::runtime::target::finish_target_recovery_failure(
                &mut handle,
                &session,
                started_at,
                &error,
            );
            return Err(error);
        }
        let (cancel_tx, mut cancel_rx) = tokio::sync::oneshot::channel();
        handle.register_cancellation(move || {
            let _ = cancel_tx.send(());
        });
        let mut pipe = TcpPipe::new(self);
        let dispatch = tokio::select! {
            result = pipe.dispatch(TcpPipeInput { session: &mut session }) => result,
            _ = &mut cancel_rx => {
                let reason = handle.cancellation_reason().unwrap_or_else(|| "cancelled".to_owned());
                let _ = handle.finish_with_reason(SessionOutcome::Cancelled, Some(reason));
                return Ok(MessageRelayOutcome::Close);
            }
        };
        let result = match dispatch {
            Ok(result) => result,
            Err(error) if is_block_error(&error) => {
                let _ = protocol.send_blocked(client).await;
                finish_blocked(&mut handle);
                return Ok(MessageRelayOutcome::Close);
            }
            Err(error) => {
                let _ = protocol.send_upstream_failure(client).await;
                finish_route_or_establish_failure(&mut handle, &session, started_at, &error);
                return Err(error);
            }
        };

        self.log_session_accepted(&session, &result.route_action);
        session.outbound_tag = Some(result.outbound_tag.clone());
        self.set_session_outbound(
            &session,
            result.upstream_endpoint.as_ref(),
            result.relay_chain.clone(),
            result.network,
        );
        let outcome = if result.is_direct {
            SessionOutcome::DirectRelayed
        } else {
            SessionOutcome::ChainedRelayed
        };
        let upstream_endpoint = result.upstream_endpoint;
        let passive_relay_selections = result.passive_relay_selections;
        let relay_context = MessageRelayContext::new(
            self.runtime_services(),
            session.id,
            rate_limiters,
            self.idle_timeout(),
        );
        let relay = tokio::select! {
            result = protocol.relay(client, result.upstream, &request, relay_context) => result,
            _ = &mut cancel_rx => {
                let reason = handle.cancellation_reason().unwrap_or_else(|| "cancelled".to_owned());
                let _ = handle.finish_with_reason(SessionOutcome::Cancelled, Some(reason));
                return Ok(MessageRelayOutcome::Close);
            }
        };

        match relay {
            Ok(relay_outcome) => {
                if let Some(record) =
                    finish_relay_success(&mut handle, outcome, upstream_endpoint.as_ref())
                {
                    self.record_passive_relay_outcome(
                        &passive_relay_selections,
                        &session,
                        classify_relay_outcome(&record, None),
                    );
                }
                Ok(relay_outcome)
            }
            Err(error) => {
                if let Some(record) = finish_relay_failure(
                    &mut handle,
                    &session,
                    started_at,
                    &error,
                    upstream_endpoint.as_ref(),
                ) {
                    self.record_passive_relay_outcome(
                        &passive_relay_selections,
                        &session,
                        classify_relay_outcome(&record, Some(&error)),
                    );
                }
                Err(error)
            }
        }
    }
}

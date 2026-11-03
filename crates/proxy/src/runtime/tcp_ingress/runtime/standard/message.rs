mod error;
use std::time::Instant;

use std::sync::Arc;
use zero_core::SessionAuth;
use zero_engine::{EngineError, SessionOutcome};
use zero_platform_tokio::contract::inbound::{
    AcceptedMessage, MessageAction, MessageContext, MessageTransfer,
};

use super::super::super::contract::{TcpActivityStream, TcpRelayActivity};
use super::super::super::lifecycle::result::{
    finish_blocked, finish_relay_failure, finish_relay_idle_timeout, finish_relay_success,
    finish_route_or_establish_failure,
};
use super::super::TcpIngressRuntime;
use crate::runtime::passive_relay_health::classify_relay_outcome;
use crate::runtime::pipe::{KernelPipe, TcpPipe, TcpPipeInput};
use crate::transport::is_block_error;

impl TcpIngressRuntime {
    pub(crate) async fn serve_accepted_message(
        &self,
        accepted: Box<dyn AcceptedMessage>,
        authentication: Option<SessionAuth>,
        identity: zero_engine::SessionIdentity,
    ) -> Result<(), EngineError> {
        let mut session = self.accepted_session(accepted.facts(), authentication, &identity)?;
        if let Some((status, location)) = accepted
            .facts()
            .target
            .as_ref()
            .and_then(|target| self.select_http_redirect(target))
        {
            accepted
                .respond(MessageAction::Redirect { status, location })
                .await
                .map_err(error::into_io)?;
            return Ok(());
        }
        let started_at = Instant::now();
        let target_resolution = self.resolve_fake_ip_target(&mut session).await;
        if target_resolution.is_ok() {
            self.apply_url_rewrite(&mut session);
        }
        self.apply_kernel_rate_limits(&mut session);
        self.prepare_session_with_ticket(&mut session, Some(&identity))
            .await?;
        let rate_limiters = self.traffic_rate_limiters(&session);
        let mut handle = self.track_session(session.id);
        if let Err(error) = target_resolution {
            let _ = accepted.respond(MessageAction::UpstreamFailed).await;
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
            result = pipe.dispatch(TcpPipeInput { session: &mut session, admitted: None }) => result,
            _ = &mut cancel_rx => {
                let reason = handle.cancellation_reason().unwrap_or_else(|| "cancelled".to_owned());
                let _ = handle.finish_with_reason(SessionOutcome::Cancelled, Some(reason));
                return Ok(());
            }
        };
        let result = match dispatch {
            Ok(result) => result,
            Err(error) if is_block_error(&error) => {
                let _ = accepted.respond(MessageAction::Blocked).await;
                finish_blocked(&mut handle);
                return Ok(());
            }
            Err(error) => {
                let _ = accepted.respond(MessageAction::UpstreamFailed).await;
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
        let idle_timeout = self.idle_timeout();
        let activity = TcpRelayActivity::new();
        let relay_context = MessageContext::new(
            rate_limiters.upload(),
            rate_limiters.download(),
            activity.clone(),
            Arc::new(Boundary {
                engine: self.runtime_services().engine().clone(),
                session: session.id,
                inbound: true,
            }),
            Arc::new(Boundary {
                engine: self.runtime_services().engine().clone(),
                session: session.id,
                inbound: false,
            }),
        );
        let upgrade_context = relay_context.clone();
        let effective_target =
            zero_core::request::Destination::new(session.target.clone(), session.port);
        let transfer = async {
            match accepted
                .respond(MessageAction::Transfer {
                    upstream: result.upstream,
                    effective_target,
                    context: relay_context,
                })
                .await
                .map_err(error::into_io)?
            {
                MessageTransfer::Complete => Ok::<(), EngineError>(()),
                MessageTransfer::Upgraded {
                    mut inbound,
                    upstream,
                } => {
                    let download = upgrade_context.clone();
                    crate::transport::relay_bidirectional_metered_throttled(
                        TcpActivityStream::new(&mut inbound, activity.clone()),
                        TcpActivityStream::new(upstream, activity.clone()),
                        move |bytes| upgrade_context.record_upload(bytes),
                        move |bytes| download.record_download(bytes),
                        rate_limiters.upload(),
                        rate_limiters.download(),
                    )
                    .await?;
                    Ok(())
                }
            }
        };
        let relay = tokio::select! {
            result = transfer => Some(result),
            _ = activity.wait_for_idle(idle_timeout) => None,
            _ = &mut cancel_rx => {
                let reason = handle.cancellation_reason().unwrap_or_else(|| "cancelled".to_owned());
                let _ = handle.finish_with_reason(SessionOutcome::Cancelled, Some(reason));
                return Ok(());
            }
        };

        match relay {
            Some(Ok(())) => {
                if let Some(record) =
                    finish_relay_success(&mut handle, outcome, upstream_endpoint.as_ref())
                {
                    self.record_passive_relay_outcome(
                        &passive_relay_selections,
                        &session,
                        classify_relay_outcome(&record, None),
                    );
                }
                Ok(())
            }
            Some(Err(error)) => {
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
            None => {
                if let Some(record) =
                    finish_relay_idle_timeout(&mut handle, outcome, upstream_endpoint.as_ref())
                {
                    self.record_passive_relay_outcome(
                        &passive_relay_selections,
                        &session,
                        classify_relay_outcome(&record, None),
                    );
                }
                Ok(())
            }
        }
    }
}

struct Boundary {
    engine: zero_engine::Engine,
    session: u64,
    inbound: bool,
}
impl std::fmt::Debug for Boundary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MessageBoundary")
            .field("session", &self.session)
            .field("inbound", &self.inbound)
            .finish()
    }
}
impl zero_traits::IoObserver for Boundary {
    fn received(&self, bytes: usize) {
        if self.inbound {
            self.engine
                .record_session_inbound_rx(self.session, bytes as u64);
        } else {
            self.engine
                .record_session_outbound_rx(self.session, bytes as u64);
        }
    }
    fn sent(&self, bytes: usize) {
        if self.inbound {
            self.engine
                .record_session_inbound_tx(self.session, bytes as u64);
        } else {
            self.engine
                .record_session_outbound_tx(self.session, bytes as u64);
        }
    }
    fn error(&self) {}
    fn dropped(&self) {}
}

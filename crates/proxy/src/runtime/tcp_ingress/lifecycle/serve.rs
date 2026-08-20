use std::time::Instant;

#[cfg(any(
    feature = "upstream-association-runtime",
    feature = "managed-datagram-runtime",
    feature = "managed-stream-runtime"
))]
use tokio::io::{AsyncRead, AsyncWrite};
#[cfg(any(
    feature = "upstream-association-runtime",
    feature = "managed-datagram-runtime",
    feature = "managed-stream-runtime"
))]
use zero_core::InboundClientResponse;
use zero_core::Session;
use zero_engine::{EngineError, SessionOutcome};
#[cfg(any(
    feature = "upstream-association-runtime",
    feature = "managed-datagram-runtime",
    feature = "managed-stream-runtime"
))]
use zero_traits::AsyncSocket;

#[cfg(any(
    feature = "upstream-association-runtime",
    feature = "managed-datagram-runtime",
    feature = "managed-stream-runtime"
))]
use super::super::contract::ClientResponseInboundProtocol;
use super::super::contract::InboundProtocol;
use super::super::runtime::TcpIngressRuntime;
use super::result::{
    finish_blocked, finish_relay_failure, finish_relay_idle_timeout, finish_relay_success,
    finish_route_or_establish_failure,
};
use crate::runtime::passive_relay_health::classify_relay_outcome;
use crate::runtime::pipe::{KernelPipe, TcpPipe, TcpPipeInput};
use crate::transport::is_block_error;

#[cfg(any(
    feature = "upstream-association-runtime",
    feature = "managed-datagram-runtime",
    feature = "managed-stream-runtime"
))]
pub(crate) async fn serve_inbound_with_client_response<P, S>(
    runtime: &TcpIngressRuntime,
    session: Session,
    client: S,
    response_protocol: P,
) -> Result<(), EngineError>
where
    P: InboundClientResponse<S> + Send + Sync,
    S: AsyncRead + AsyncWrite + AsyncSocket + Unpin + Send,
{
    let protocol = ClientResponseInboundProtocol::new(response_protocol);
    serve_inbound(runtime, session, client, &protocol).await
}

pub(crate) async fn serve_inbound<P: InboundProtocol>(
    runtime: &TcpIngressRuntime,
    session: Session,
    client: P::ClientStream,
    protocol: &P,
) -> Result<(), EngineError> {
    let mut session = session;
    let mut client = client;

    runtime.resolve_fake_ip_target(&mut session).await;
    runtime.apply_url_rewrite(&mut session);
    runtime.apply_kernel_rate_limits(&mut session);
    runtime.prepare_session(&mut session).await?;
    let traffic_rate_limiters = runtime.traffic_rate_limiters(&session);

    let mut handle = runtime.track_session(session.id);
    let (cancel_tx, mut cancel_rx) = tokio::sync::oneshot::channel();
    handle.register_cancellation(move || {
        let _ = cancel_tx.send(());
    });
    let started_at = Instant::now();

    let mut pipe = TcpPipe::new(runtime);
    let dispatch_result = tokio::select! {
        result = pipe.dispatch(TcpPipeInput { session: &mut session }) => result,
        _ = &mut cancel_rx => {
            let reason = handle.cancellation_reason().unwrap_or_else(|| "cancelled".to_owned());
            let _ = handle.finish_with_reason(SessionOutcome::Cancelled, Some(reason));
            return Ok(());
        }
    };

    match dispatch_result {
        Ok(result) => {
            runtime.log_session_accepted(&session, &result.route_action);

            session.outbound_tag = Some(result.outbound_tag.clone());
            runtime.set_session_outbound(
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

            protocol.send_ok(&mut client).await?;

            let relay_result = tokio::select! {
                result = tokio::time::timeout(
                    runtime.idle_timeout(),
                    protocol.relay(
                        client,
                        result.upstream,
                        runtime.runtime_services(),
                        session.id,
                        traffic_rate_limiters,
                    ),
                ) => result,
                _ = &mut cancel_rx => {
                    let reason = handle.cancellation_reason().unwrap_or_else(|| "cancelled".to_owned());
                    let _ = handle.finish_with_reason(SessionOutcome::Cancelled, Some(reason));
                    return Ok(());
                }
            };

            match relay_result {
                Ok(Ok(())) => {
                    if let Some(record) =
                        finish_relay_success(&mut handle, outcome, upstream_endpoint.as_ref())
                    {
                        runtime.record_passive_relay_outcome(
                            &passive_relay_selections,
                            &session,
                            classify_relay_outcome(&record, None),
                        );
                    }
                    Ok(())
                }
                Ok(Err(error)) => {
                    if let Some(record) = finish_relay_failure(
                        &mut handle,
                        &session,
                        started_at,
                        &error,
                        upstream_endpoint.as_ref(),
                    ) {
                        runtime.record_passive_relay_outcome(
                            &passive_relay_selections,
                            &session,
                            classify_relay_outcome(&record, Some(&error)),
                        );
                    }
                    Err(error)
                }
                Err(_elapsed) => {
                    if let Some(record) =
                        finish_relay_idle_timeout(&mut handle, outcome, upstream_endpoint.as_ref())
                    {
                        runtime.record_passive_relay_outcome(
                            &passive_relay_selections,
                            &session,
                            classify_relay_outcome(&record, None),
                        );
                    }
                    Ok(())
                }
            }
        }
        Err(error) if is_block_error(&error) => {
            let _ = protocol.send_blocked(&mut client).await;
            finish_blocked(&mut handle);
            Ok(())
        }
        Err(error) => {
            let _ = protocol.send_upstream_failure(&mut client).await;
            finish_route_or_establish_failure(&mut handle, &session, started_at, &error);
            Err(error)
        }
    }
}

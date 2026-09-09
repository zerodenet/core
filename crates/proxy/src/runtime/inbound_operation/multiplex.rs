//! Runtime task ownership for protocol-owned stream/datagram multiplexers.

use tokio::io::{AsyncRead, AsyncWrite};
use zero_core::InboundDatagramMultiplexer;
use zero_engine::EngineError;

use super::InboundConnectionContext;
use crate::runtime::route_runtime::InboundRouteRuntime;

pub(super) async fn run_datagram_multiplexer<C>(
    runtime: InboundRouteRuntime,
    connection: C,
) -> Result<(), EngineError>
where
    C: InboundDatagramMultiplexer,
    C::Stream: AsyncRead + AsyncWrite,
    C::Error: Into<EngineError>,
{
    let device_registration = match runtime.acquire_principal_device(connection.auth()) {
        Ok(registration) => registration,
        Err(error) => {
            connection.close("device_limit");
            return Err(error);
        }
    };
    let (principal_cancel_tx, mut principal_cancel_rx) =
        tokio::sync::mpsc::unbounded_channel::<String>();
    let principal_registration = connection
        .auth()
        .and_then(|auth| auth.principal_key.as_deref())
        .map(|principal_key| {
            runtime.register_principal_cancellation(principal_key, move |reason| {
                let _ = principal_cancel_tx.send(reason);
            })
        });
    let mut tasks = tokio::task::JoinSet::new();
    let udp_source = connection.datagram_source();
    let udp_relay = connection.udp_relay();
    let udp_runtime = runtime.udp_runtime();
    let udp_tag = runtime.inbound_tag().to_owned();
    tasks.spawn(async move {
        crate::runtime::datagram_udp::run_protocol_datagram_udp_relay(
            udp_runtime,
            udp_source,
            udp_relay,
            &udp_tag,
            false,
        )
        .await
    });

    loop {
        tokio::select! {
            accepted = connection.accept_next_tcp_stream() => {
                let Some((session, stream)) = accepted.map_err(Into::into)? else {
                    break;
                };
                let context = InboundConnectionContext::new(runtime.clone());
                let response = connection.response_protocol();
                tasks.spawn(async move {
                    context.serve_with_client_response(session, stream, response).await
                });
            }
            result = tasks.join_next(), if !tasks.is_empty() => {
                match result {
                    Some(Ok(Ok(()))) => {}
                    Some(Ok(Err(error))) => tracing::warn!(%error, "inbound multiplexed stream task failed"),
                    Some(Err(error)) if !error.is_cancelled() => {
                        tracing::error!(%error, "inbound multiplexed stream task panicked");
                    }
                    Some(Err(_)) | None => {}
                }
            }
            Some(reason) = principal_cancel_rx.recv() => {
                connection.close(&reason);
                break;
            }
        }
    }

    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    drop(principal_registration);
    drop(device_registration);
    Ok(())
}

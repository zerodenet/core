//! Runtime-owned logical session handshakes, policy and task cleanup.
use super::InboundConnectionContext;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use zero_core::{InboundRouteMultiplexer, InboundStreamRoute, InboundStreamUdpRelay};
use zero_engine::EngineError;

impl InboundConnectionContext {
    #[allow(dead_code)]
    pub(crate) async fn run_route_multiplexer<C>(
        self,
        connection: C,
        udp_protocol: &'static str,
    ) -> Result<(), EngineError>
    where
        C: InboundRouteMultiplexer,
        C::Error: Into<EngineError>,
        <C::Route as InboundStreamRoute>::TcpStream:
            AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static,
        <<C::Route as InboundStreamRoute>::UdpRelay as InboundStreamUdpRelay>::Stream:
            AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static,
    {
        let connection = Arc::new(connection);
        let device = match self.runtime.acquire_principal_device(connection.auth()) {
            Ok(registration) => registration,
            Err(error) => {
                connection.close("device_limit");
                return Err(error);
            }
        };
        let (cancel_tx, mut cancel_rx) = tokio::sync::mpsc::unbounded_channel();
        let principal = connection
            .auth()
            .and_then(|auth| auth.principal_key.as_deref())
            .map(|key| {
                self.runtime
                    .register_principal_cancellation(key, move |reason| {
                        let _ = cancel_tx.send(reason);
                    })
            });
        let mut tasks = tokio::task::JoinSet::new();
        let outcome = loop {
            tokio::select! {
                next = connection.accept_next(), if tasks.len() < 256 => {
                    match next {
                        Ok(Some(incoming)) => {
                            let connection = connection.clone();
                            let context = self.clone();
                            tasks.spawn(async move {
                                let route = tokio::time::timeout(std::time::Duration::from_secs(30), connection.accept_route(incoming))
                                    .await.map_err(|_| EngineError::Io(std::io::Error::new(std::io::ErrorKind::TimedOut, "logical stream handshake timed out")))?
                                    .map_err(Into::into)?;
                                context.dispatch_stream_route_with_client_response(route, connection.response_protocol(), udp_protocol).await
                            });
                        }
                        Ok(None) => break Ok(()),
                        Err(error) => break Err(error.into()),
                    }
                }
                Some(reason) = cancel_rx.recv() => { connection.close(&reason); break Ok(()); }
                result = tasks.join_next(), if !tasks.is_empty() => {
                    match result {
                        Some(Ok(Err(error))) => tracing::warn!(%error, "inbound logical session failed"),
                        Some(Err(error)) if !error.is_cancelled() => tracing::error!(%error, "inbound logical session panicked"),
                        _ => {}
                    }
                }
            }
        };
        connection.close("runtime_exit");
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        drop(principal);
        drop(device);
        outcome
    }
}

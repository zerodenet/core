//! Runtime owns per-stream route tasks for multiplexed carriers.
use super::InboundConnectionContext;
use std::future::Future;
use zero_core::InboundTransportMultiplexer;
use zero_engine::EngineError;

impl InboundConnectionContext {
    pub(crate) async fn run_transport_streams<C, F, Fut>(
        self,
        source: C,
        dispatch: F,
    ) -> Result<(), EngineError>
    where
        C: InboundTransportMultiplexer,
        F: Fn(C::Stream, Self) -> Fut,
        Fut: Future<Output = Result<(), EngineError>> + Send + 'static,
    {
        let mut tasks = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                stream = source.accept_stream(), if tasks.len() < 256 => {
                    let Some(stream) = stream else { break; };
                    tasks.spawn(dispatch(stream, self.clone()));
                }
                result = tasks.join_next(), if !tasks.is_empty() => {
                    match result {
                        Some(Ok(Err(error))) => tracing::warn!(%error, "inbound transport stream failed"),
                        Some(Err(error)) if !error.is_cancelled() => tracing::error!(%error, "inbound transport stream panicked"),
                        _ => {}
                    }
                }
            }
        }
        source.close();
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        Ok(())
    }
}

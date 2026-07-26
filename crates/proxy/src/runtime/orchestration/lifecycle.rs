use std::future::Future;
use std::io;

use zero_engine::EngineError;

use super::logging::log_stopped;
use super::state::OrchestrationState;
use crate::runtime::Proxy;

pub(in crate::runtime) async fn run_until<F>(proxy: &Proxy, shutdown: F) -> Result<(), EngineError>
where
    F: Future<Output = ()> + Send,
{
    if proxy.config.inbounds.is_empty() {
        return Err(EngineError::NoInbounds);
    }

    let mut state = OrchestrationState::new(proxy).await?;
    tokio::pin!(shutdown);
    let mut shutting_down = false;

    loop {
        if shutting_down && state.is_idle() {
            log_stopped(proxy);
            return Ok(());
        }

        tokio::select! {
            _ = &mut shutdown, if !shutting_down => {
                shutting_down = true;
                state.propagate_shutdown();
            }
            Some(()) = state.reload_async_rx.recv() => {
                if shutting_down {
                    continue;
                }
                state.reconcile_reload(proxy).await;
            }
            result = state.listeners.join_next(), if !state.listeners.is_empty() => {
                handle_listener_result(result, shutting_down, &mut state.expected_listener_exits)?;
            }
            result = state.urltests.join_next(), if !state.urltests.is_empty() => {
                handle_urltest_result(result, shutting_down)?;
            }
        }
    }
}

fn handle_listener_result(
    result: Option<Result<Result<(), EngineError>, tokio::task::JoinError>>,
    shutting_down: bool,
    expected_exits: &mut usize,
) -> Result<(), EngineError> {
    match result {
        Some(Ok(Ok(()))) if shutting_down => Ok(()),
        Some(Ok(Ok(()))) if *expected_exits > 0 => {
            *expected_exits -= 1;
            Ok(())
        }
        Some(Ok(Ok(()))) => Err(EngineError::InboundTaskExited),
        Some(Ok(Err(error))) => Err(error),
        Some(Err(error)) => Err(io::Error::other(error).into()),
        None if shutting_down => Ok(()),
        None => Err(EngineError::InboundTaskExited),
    }
}

fn handle_urltest_result(
    result: Option<Result<Result<(), EngineError>, tokio::task::JoinError>>,
    shutting_down: bool,
) -> Result<(), EngineError> {
    match result {
        Some(Ok(Ok(()))) if shutting_down => Ok(()),
        Some(Ok(Ok(()))) => Err(EngineError::UrlTestTaskExited),
        Some(Ok(Err(error))) => Err(error),
        Some(Err(error)) => Err(io::Error::other(error).into()),
        None if shutting_down => Ok(()),
        None => Err(EngineError::UrlTestTaskExited),
    }
}

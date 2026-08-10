use std::future::Future;
use std::io;

use tracing::{error, info};
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
                info!(
                    core_instance_id = proxy.core_instance_id(),
                    config_revision = proxy.config_revision(),
                    reason = "shutdown_signal",
                    "proxy orchestration shutdown requested"
                );
                state.propagate_shutdown();
            }
            Some(()) = state.reload_async_rx.recv() => {
                if shutting_down {
                    continue;
                }
                info!(
                    core_instance_id = proxy.core_instance_id(),
                    config_revision = proxy.config_revision(),
                    reason = "config_reload",
                    "proxy orchestration reload requested"
                );
                state.reconcile_reload(proxy).await;
            }
            result = state.listeners.join_next(), if !state.listeners.is_empty() => {
                if let Err(listener_error) = handle_listener_result(
                    result,
                    shutting_down,
                    &mut state.expected_listener_exits,
                ) {
                    error!(
                        core_instance_id = proxy.core_instance_id(),
                        config_revision = proxy.config_revision(),
                        expected_listener_exits = state.expected_listener_exits,
                        active_listener_tasks = state.listeners.len(),
                        reason = "listener_task_exit",
                        error = %listener_error,
                        "proxy orchestration observed unexpected inbound listener termination"
                    );
                    return Err(listener_error);
                }
            }
            result = state.urltests.join_next(), if !state.urltests.is_empty() => {
                if let Err(urltest_error) = handle_urltest_result(result, shutting_down) {
                    error!(
                        core_instance_id = proxy.core_instance_id(),
                        config_revision = proxy.config_revision(),
                        active_listener_tasks = state.listeners.len(),
                        active_urltest_tasks = state.urltests.len(),
                        reason = "urltest_task_exit",
                        error = %urltest_error,
                        "proxy orchestration observed unexpected urltest termination"
                    );
                    return Err(urltest_error);
                }
            }
        }
    }
}

pub(super) fn handle_listener_result(
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

pub(super) fn handle_urltest_result(
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

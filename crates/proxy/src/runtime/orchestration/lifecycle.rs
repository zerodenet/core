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
    // An empty configuration is a valid management-only runtime. Keep the
    // reload/shutdown loop alive so a controller can install its first inbound
    // through the same reconciled configuration transaction used on reload.
    let mut state = OrchestrationState::new(proxy).await?;
    let _sampler = super::statistics::Sampler::start(proxy.engine.clone());
    let result = run_loop(proxy, &mut state, shutdown).await;
    let result = state.finish(proxy, result).await;
    log_stopped(proxy);
    result
}

pub(super) async fn run_loop<F>(
    proxy: &Proxy,
    state: &mut OrchestrationState,
    shutdown: F,
) -> Result<(), EngineError>
where
    F: Future<Output = ()> + Send,
{
    tokio::pin!(shutdown);
    loop {
        #[cfg(feature = "raw-ip-runtime")]
        let network_change = {
            let retry_at = state.device_retry_at;
            let changes = &mut state.egress_updates;
            async move {
                tokio::select! {
                    Ok(()) = changes.changed() => {},
                    _ = async {
                        if let Some(deadline) = retry_at {
                            tokio::time::sleep_until(deadline).await;
                        } else {
                            std::future::pending::<()>().await;
                        }
                    } => {},
                }
            }
        };
        #[cfg(not(feature = "raw-ip-runtime"))]
        let network_change = std::future::pending::<()>();

        tokio::select! {
            _ = &mut shutdown => {
                info!(
                    core_instance_id = proxy.core_instance_id(),
                    config_revision = proxy.config_revision(),
                    reason = "shutdown_signal",
                    "proxy orchestration shutdown requested"
                );
                return Ok(());
            }
            Some(()) = state.reload_async_rx.recv() => {
                info!(
                    core_instance_id = proxy.core_instance_id(),
                    config_revision = proxy.config_revision(),
                    reason = "config_reload",
                    "proxy orchestration reload requested"
                );
                state.reconcile_reload(proxy).await;
            }
            _ = network_change => {
                #[cfg(feature = "raw-ip-runtime")]
                state.reconcile_outbound_network(proxy).await;
            }
            result = state.listeners.join_next(), if !state.listeners.is_empty() => {
                if let Err(listener_error) = handle_listener_result(
                    result,
                    false,
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
                if let Err(urltest_error) = handle_urltest_result(result, false) {
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
            result = state.services.join_next(), if !state.services.is_empty() => {
                match result {
                    Some(Ok(Err(error))) => return Err(error),
                    Some(Err(error)) => return Err(io::Error::other(error).into()),
                    _ => return Err(io::Error::other("background ingress service exited unexpectedly").into()),
                }
            }
            failure = state.configured_tun_failures.recv() => {
                if let Err(error) = handle_configured_tun_failure(failure) {
                    error!(
                        core_instance_id = proxy.core_instance_id(),
                        config_revision = proxy.config_revision(),
                        reason = "configured_tun_task_exit",
                        error = %error,
                        "proxy orchestration observed configured TUN termination"
                    );
                    state.propagate_shutdown();
                    return Err(error);
                }
            }
        }
    }
}

pub(super) fn handle_configured_tun_failure(
    result: Result<String, tokio::sync::broadcast::error::RecvError>,
) -> Result<(), EngineError> {
    match result {
        Ok(message) => Err(EngineError::Io(io::Error::other(format!(
            "configured TUN runtime failed: {message}"
        )))),
        Err(tokio::sync::broadcast::error::RecvError::Lagged(_))
        | Err(tokio::sync::broadcast::error::RecvError::Closed) => Ok(()),
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

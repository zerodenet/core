use tokio::task::JoinSet;
use tracing::{info, warn};

use super::model::{MuxOpenedDispatcher, MuxSessionLoop};

pub(crate) async fn run_mux_session_loop<D>(
    request: MuxSessionLoop<'_>,
    tasks: &mut JoinSet<()>,
    dispatcher: &mut D,
    principal_cancel_rx: &mut tokio::sync::mpsc::UnboundedReceiver<String>,
) -> Result<(), D::Error>
where
    D: MuxOpenedDispatcher,
{
    info!(
        inbound_tag = request.inbound_tag,
        protocol = request.protocol,
        "mux session started"
    );

    loop {
        tokio::select! {
            dispatched = dispatcher.dispatch_next(tasks) => {
                if !dispatched? {
                    break;
                }
            }
            Some(reason) = principal_cancel_rx.recv() => {
                info!(
                    inbound_tag = request.inbound_tag,
                    protocol = request.protocol,
                    reason,
                    "mux session cancelled for principal"
                );
                break;
            }
        }

        drain_completed_mux_tasks(tasks, request.panic_message);
    }

    info!(
        inbound_tag = request.inbound_tag,
        protocol = request.protocol,
        "mux session ended"
    );
    Ok(())
}

pub(crate) async fn finish_mux_tasks(
    tasks: &mut JoinSet<()>,
    graceful_timeout: std::time::Duration,
    panic_message: &'static str,
) {
    let deadline = tokio::time::Instant::now() + graceful_timeout;
    while !tasks.is_empty() {
        tokio::select! {
            joined = tasks.join_next() => {
                let Some(joined) = joined else {
                    break;
                };
                if let Err(error) = joined {
                    warn!(error = %error, panic_message);
                }
            }
            _ = tokio::time::sleep_until(deadline) => break,
        }
    }

    if tasks.is_empty() {
        return;
    }
    tasks.abort_all();
    while let Some(joined) = tasks.join_next().await {
        if let Err(error) = joined {
            if !error.is_cancelled() {
                warn!(error = %error, panic_message);
            }
        }
    }
}

pub(crate) fn drain_completed_mux_tasks(tasks: &mut JoinSet<()>, panic_message: &'static str) {
    while let Some(joined) = tasks.try_join_next() {
        if let Err(error) = joined {
            warn!(error = %error, panic_message);
        }
    }
}

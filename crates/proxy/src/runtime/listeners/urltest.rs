use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing::{error, info};
use zero_engine::EngineError;

use crate::groups::UrlTestRuntime;

pub(in crate::runtime) async fn reconcile_urltests(
    runtime: &UrlTestRuntime,
    shutdown_rx: &watch::Receiver<bool>,
    urltests: &mut JoinSet<Result<(), EngineError>>,
) {
    let previous_tasks = urltests.len();
    urltests.abort_all();
    while urltests.join_next().await.is_some() {}
    runtime.clear_probe_triggers();

    info!(
        previous_tasks = previous_tasks,
        reason = "config_reload",
        "reconciled previous urltest runtime tasks"
    );

    let group_ids = runtime.group_ids();

    for group_id in group_ids {
        let runtime = runtime.clone();
        let shutdown = shutdown_rx.clone();
        urltests.spawn(async move {
            info!(
                group_id = group_id,
                reason = "config_reload",
                "urltest runtime task started"
            );
            let result = runtime.run_urltest_group(group_id, shutdown).await;
            match &result {
                Ok(()) => info!(
                    group_id = group_id,
                    reason = "urltest_task_returned",
                    "urltest runtime task returned"
                ),
                Err(urltest_error) => error!(
                    group_id = group_id,
                    reason = "urltest_task_error",
                    error = %urltest_error,
                    "urltest runtime task failed"
                ),
            }
            result
        });
    }
}

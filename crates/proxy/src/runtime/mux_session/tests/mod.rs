use std::convert::Infallible;

use tokio::task::JoinSet;

use super::lifecycle::{finish_mux_tasks, run_mux_session_loop};
use super::model::{MuxOpenedDispatcher, MuxSessionLoop};

struct PendingDispatcher;

impl MuxOpenedDispatcher for PendingDispatcher {
    type Error = Infallible;

    async fn dispatch_next(&mut self, _tasks: &mut JoinSet<()>) -> Result<bool, Self::Error> {
        std::future::pending().await
    }
}

#[tokio::test]
async fn principal_cancellation_ends_mux_carrier_and_aborts_substreams() {
    let (cancel_tx, mut cancel_rx) = tokio::sync::mpsc::unbounded_channel();
    let task_finished = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let task_finished_clone = task_finished.clone();
    let (task_started_tx, task_started_rx) = tokio::sync::oneshot::channel();
    let mut tasks = JoinSet::new();
    tasks.spawn(async move {
        struct MarkFinished(std::sync::Arc<std::sync::atomic::AtomicBool>);
        impl Drop for MarkFinished {
            fn drop(&mut self) {
                self.0.store(true, std::sync::atomic::Ordering::Release);
            }
        }
        let _mark_finished = MarkFinished(task_finished_clone);
        let _ = task_started_tx.send(());
        std::future::pending::<()>().await;
    });
    task_started_rx.await.expect("substream task started");
    cancel_tx
        .send("principal_disabled".to_owned())
        .expect("queue cancellation");
    let mut dispatcher = PendingDispatcher;

    run_mux_session_loop(
        MuxSessionLoop {
            inbound_tag: "vmess-in",
            protocol: "vmess_mux",
            panic_message: "mux substream panicked",
            abort_on_end: true,
        },
        &mut tasks,
        &mut dispatcher,
        &mut cancel_rx,
    )
    .await
    .expect("cancel mux session");

    finish_mux_tasks(
        &mut tasks,
        std::time::Duration::from_millis(10),
        "mux substream panicked",
    )
    .await;
    assert!(task_finished.load(std::sync::atomic::Ordering::Acquire));
}

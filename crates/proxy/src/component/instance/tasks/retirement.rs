//! Publication to the actual persistent cleanup receiver, not a second join loop.
use super::*;

impl TaskMonitor {
    pub(in crate::component) async fn collect(
        &self,
        deadline: Instant,
        mut persist: impl FnMut(CloseReport),
    ) {
        self.begin_shutdown();
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let mut retired = Vec::new();
            let drained = {
                let mut tasks = self.lock();
                let mut report = std::mem::take(&mut tasks.report);
                for receipt in &tasks.pending {
                    report.exited_tasks.push(receipt.id());
                    if let Err(source) = receipt.result() {
                        report.failed_tasks.push(TaskFailure {
                            id: receipt.id(),
                            class: receipt.class(),
                            error: retained(source),
                        });
                    }
                }
                if !report.exited_tasks.is_empty() || !report.failed_tasks.is_empty() {
                    // The same persistent lifecycle owner owns this report before
                    // any actual scope credit can be returned or a future dropped.
                    persist(report);
                    let pending = std::mem::take(&mut tasks.pending);
                    for receipt in pending {
                        tasks
                            .scope
                            .acknowledge(&receipt)
                            .expect("same hosted task receipt");
                        retired.push(receipt);
                    }
                }
                tasks.scope.is_drained()
            };
            drop(retired);
            if drained {
                return;
            }
            if tokio::time::timeout_at(deadline, &mut changed)
                .await
                .is_err()
            {
                let tasks = self.lock();
                let mut unfinished: Vec<_> = tasks
                    .scope
                    .unjoined_tasks()
                    .into_iter()
                    .map(Unfinished::Task)
                    .collect();
                if unfinished.is_empty() && !tasks.scope.is_drained() {
                    unfinished.push(Unfinished::Recovery(
                        "joined component exits await host dispatch".into(),
                    ));
                }
                drop(tasks);
                persist(CloseReport {
                    unfinished,
                    ..Default::default()
                });
                return;
            }
        }
    }
}

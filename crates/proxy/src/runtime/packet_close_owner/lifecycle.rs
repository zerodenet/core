use super::*;
impl PacketCloseOwner {
    pub(in crate::runtime) fn begin_shutdown(&self) {
        let _accept = self
            .accept_gate
            .lock()
            .expect("packet close admission poisoned");
        self.closing.store(true, Ordering::Release);
        self.changed.notify_one();
    }
    pub(in crate::runtime) async fn observe_failure(&self) -> String {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.completed.load(Ordering::Acquire) > 0 {
                let mut owner = self.owner.lock().await;
                if let Err(error) = self.reap(&mut owner).await {
                    return error;
                }
            }
            changed.await;
        }
    }
    pub(in crate::runtime) async fn shutdown(&self, deadline: Instant) -> Result<(), String> {
        self.begin_shutdown();
        tokio::time::timeout_at(deadline, async {
            let mut owner = self.owner.lock().await;
            // Keep every actual exit in persistent owner state as it arrives.
            let Some(scope) = owner.scope.as_mut() else {
                return owner.result();
            };
            scope.begin_shutdown();
            while owner.active > 0 {
                let exit = owner
                    .scope
                    .as_mut()
                    .expect("accepted packet close scope")
                    .next_exit()
                    .await
                    .ok_or_else(|| "packet close exit remains unobserved".to_owned())?;
                owner.record(exit);
                self.completed.fetch_sub(1, Ordering::AcqRel);
            }
            let report = owner
                .scope
                .as_mut()
                .expect("accepted packet close scope")
                .shutdown(deadline)
                .await;
            if !report.is_complete() {
                return Err("packet close shutdown remains unconfirmed".to_owned());
            }
            owner.failures.extend(report.failed_tasks);
            owner.result()
        })
        .await
        .map_err(|_| "packet close shutdown remains unconfirmed".to_owned())?
    }
}

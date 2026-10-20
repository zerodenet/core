use super::*;
use tokio::time::timeout_at;
use zero_platform_tokio::contract::lifecycle::Unfinished;
impl HostResources {
    /// Join actual file jobs. A timeout retains their handles and any already
    /// collected receipts; it does not abort or report a still-running job closed.
    pub(in crate::component) async fn shutdown_io(
        &self,
        deadline: Instant,
    ) -> Result<CloseReport, ChannelError> {
        {
            let mut state = self.state.lock().expect("native grants poisoned");
            state.closed = true;
            state.start_deadline = None;
        }
        self.changed.notify_waiters();
        while matches!(timeout_at(deadline, self.next_file_exit()).await, Ok(true)) {}
        let mut state = self.state.lock().expect("native grants poisoned");
        if let Some(source) = &state.files.failure {
            return Err(crate::component::instance::retained(source.clone()));
        }
        let mut report = std::mem::take(&mut state.files.receipts);
        report
            .unfinished
            .extend(state.files.records.keys().map(|id| {
                Unfinished::Resource(format!(
                    "component {} file read task {id} remains unjoined",
                    self.component.0
                ))
            }));
        Ok(report)
    }
}

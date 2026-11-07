//! Structured cleanup of the exact providers held by these declarations.
use super::*;
use zero_platform_tokio::contract::lifecycle::Unfinished;
impl BoundDeclarations {
    fn take_returned_io(&self) -> zero_platform_tokio::contract::lifecycle::CloseReport {
        let mut report = zero_platform_tokio::contract::lifecycle::CloseReport::default();
        for entry in self.entries.values() {
            // Observe only this provider's already matching native receipts,
            // returned views and actual driver joins. No new close or IO.
            report.append(entry.provider.take_closed_io());
        }
        report
    }
    pub(in crate::component::instance::resources::native) fn revoke(&self) {
        for entry in self.entries.values() {
            entry.provider.revoke();
        }
    }

    pub(in crate::component::instance::resources::native) async fn shutdown(
        &self,
        host: &HostResources,
        deadline: Instant,
    ) -> Option<ChannelError> {
        let mut first = None;
        for (ordinal, entry) in &self.entries {
            let result = entry.provider.shutdown_scoped_io(deadline).await;
            let mut receipts = host
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match result {
                Ok(report) => receipts.io.receipts.append(report),
                Err(error) if matches!(error.reason, Reason::Revoked | Reason::Timeout) => receipts.io.receipts.unfinished.push(Unfinished::Resource(format!("component {} Bound UDP declaration {ordinal} awaits its original wire/view", self.component.0))),
                Err(error) => { first.get_or_insert(error); }
            }
        }
        first
    }
}
impl HostResources {
    pub(in crate::component) fn take_returned_bound_io(
        &self,
    ) -> zero_platform_tokio::contract::lifecycle::CloseReport {
        self.bounds.take_returned_io()
    }
}

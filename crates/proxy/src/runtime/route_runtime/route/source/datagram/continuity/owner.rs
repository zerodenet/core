//! One retained owner combines the real association graph and dispatcher originals.
use super::*;
use zero_platform_tokio::{contract::lifecycle::CloseReport, AcceptedPeerScope};
pub(super) struct Owner {
    pub graph: AcceptedPeerScope,
    pub dispatch: super::super::owner::Owner,
    pub host: Arc<Host>,
    pub association: Arc<Association>,
    pub receipts: CloseReport,
}
impl ResourceLifecycle for Owner {
    async fn start(&mut self, _: tokio::time::Instant) -> Result<(), ChannelError> {
        Ok(())
    }
    fn revoke(&self, reason: CloseReason) {
        self.association.closing.store(true, Ordering::Release);
        self.dispatch.revoke(reason);
        self.graph.revoke(reason);
    }
    async fn shutdown(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<CloseReport, ChannelError> {
        self.revoke(CloseReason::Shutdown);
        let mut failure = None;
        let mut unfinished = Vec::new();
        match self.dispatch.shutdown(deadline).await {
            Ok(mut report) => {
                unfinished.append(&mut report.unfinished);
                self.receipts.append(report);
            }
            Err(error) => {
                failure = Some(error);
            }
        }
        match self.graph.shutdown(deadline).await {
            Ok(mut report) => {
                unfinished.append(&mut report.unfinished);
                self.receipts.append(report);
            }
            Err(error) => {
                if failure.is_none() {
                    failure = Some(error);
                }
            }
        }
        if let Some(error) = failure {
            return Err(error);
        }
        let mut report = std::mem::take(&mut self.receipts);
        report.unfinished = unfinished;
        if report.is_complete() && report.failed_tasks.is_empty() {
            self.host.registry.finish(
                &self.association.scope,
                self.association.generation.load(Ordering::Acquire),
            );
        }
        Ok(report)
    }
}

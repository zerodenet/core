//! One component source polls its finite declared listeners without extra workers.
use super::*;
use futures_util::future::select_all;
use zero_platform_tokio::contract::{
    context::AcceptContext, lifecycle::*, ChannelError, CloseReason,
};
pub(super) struct Group {
    pub sources: Vec<Box<dyn DynInboundSource>>,
}
impl InboundSource for Group {
    type Handshake = PendingInbound;
    async fn next(
        &mut self,
        context: AcceptContext,
    ) -> Result<Option<PendingInbound>, ChannelError> {
        let futures: Vec<_> = self
            .sources
            .iter_mut()
            .map(|source| source.next(context.clone()))
            .collect();
        let (result, index, remaining) = select_all(futures).await;
        drop(remaining);
        let length = self.sources.len();
        self.sources.rotate_left((index + 1) % length);
        result
    }
}
pub(super) struct Lifecycle {
    pub owners: Vec<Box<dyn DynResourceLifecycle>>,
}
impl ResourceLifecycle for Lifecycle {
    async fn start(&mut self, deadline: tokio::time::Instant) -> Result<(), BuildError> {
        for owner in &mut self.owners {
            owner.start(deadline).await?;
        }
        Ok(())
    }
    fn revoke(&self, reason: CloseReason) {
        for owner in &self.owners {
            owner.revoke(reason);
        }
    }
    async fn shutdown(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<CloseReport, BuildError> {
        // Each original protocol lifecycle retains its own partial close state.
        // The host concurrently joins the one actual Scope and original providers.
        let mut report = CloseReport::default();
        for owner in self.owners.iter_mut().rev() {
            report.append(owner.shutdown(deadline).await?);
        }
        Ok(report)
    }
}

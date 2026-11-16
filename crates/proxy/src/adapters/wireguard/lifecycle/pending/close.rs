//! Join the candidate's actual virtual stacks; native receipts stay with Source.
use super::*;
use zero_platform_tokio::contract::lifecycle::CloseReport;

pub(in crate::adapters::wireguard) struct PendingPeerCloseReport {
    pub(in crate::adapters::wireguard) workers_drained: bool,
    pub(in crate::adapters::wireguard) stacks: Vec<CloseReport>,
    expected_stacks: usize,
    pub(in crate::adapters::wireguard) failures: Vec<Arc<EngineError>>,
}
impl PendingPeerCloseReport {
    pub(in crate::adapters::wireguard) fn is_complete(&self) -> bool {
        self.workers_drained
            && self.stacks.len() == self.expected_stacks
            && self.stacks.iter().all(CloseReport::is_complete)
    }
}
impl PeerResources {
    /// This graph owns the virtual clients, not the listener or outer carriers.
    /// Source shutdown separately consumes its original views, wire/reconnect
    /// joins and complete actual donor/controller/native attempt book.
    pub(in crate::adapters::wireguard) async fn stop_peer_resources(
        &self,
        deadline: tokio::time::Instant,
    ) -> PendingPeerCloseReport {
        if !self.owned {
            return PendingPeerCloseReport {
                workers_drained: false,
                stacks: Vec::new(),
                expected_stacks: self.peer_stacks.len(),
                failures: vec![Arc::new(super::super::invalid(
                    "linked graph has no candidate stack owner",
                ))],
            };
        }
        self.cancelled.cancel();
        for stack in &self.peer_stacks {
            stack.revoke();
        }
        let mut stacks = Vec::with_capacity(self.peer_stacks.len());
        let mut failures = Vec::new();
        for stack in &self.peer_stacks {
            match stack.stop(deadline).await {
                Ok(report) => stacks.push(report),
                Err(error) => failures.push(Arc::new(error)),
            }
        }
        PendingPeerCloseReport {
            workers_drained: self.peer_stacks.iter().all(|stack| stack.is_drained()),
            stacks,
            expected_stacks: self.peer_stacks.len(),
            failures,
        }
    }
}

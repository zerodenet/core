//! Test-only receipt handoff through this actual installed Host task owner.
use super::*;
use zero_platform_tokio::contract::scope::{HostedReceiptError, TaskJoinWitness};
impl TaskMonitor {
    pub(in crate::component) fn poll_held_exit_for_test(
        &self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<HostedExitReceipt>> {
        self.lock().scope.poll_exit(cx)
    }
    pub(in crate::component) fn retain_and_ack_exit_for_test(
        &self,
        witness: &TaskJoinWitness,
        receipt: &HostedExitReceipt,
    ) -> Result<(), HostedReceiptError> {
        if !self.identity.same_scope(&witness.scope_identity()) {
            return Err(HostedReceiptError::ForeignScope);
        }
        if !witness.matches_joined(receipt.joined()) {
            return Err(HostedReceiptError::ForeignTask);
        }
        let result = {
            let mut actual = self.lock();
            // Exact actual outcome is strong in this same persistent Host
            // report before its non-cloneable receipt can release any credit.
            if !actual.report.exited_tasks.contains(&receipt.id()) {
                actual.report.exited_tasks.push(receipt.id());
                if let Err(source) = receipt.result() {
                    actual.report.failed_tasks.push(TaskFailure {
                        id: receipt.id(),
                        class: receipt.class(),
                        error: retained(source),
                    });
                }
            }
            actual.scope.acknowledge(receipt)
        };
        self.changed.notify_waiters();
        result
    }
}

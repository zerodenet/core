//! The same real primary's prepaid record survives its accepted callee.
use super::*;
impl HostPeerBinding {
    pub(in crate::runtime::inbound_owners) fn install_task_failure(
        &self,
        original: zero_platform_tokio::AcceptedTaskFailureRegistration,
    ) {
        self.join.state.lock().unwrap().task_failure = Some(original.clone());
        self.task_failure
            .set(original)
            .unwrap_or_else(|_| panic!("one actual prepaid peer task registration"));
    }
    pub(in crate::runtime::inbound_owners) fn receive_primary_joined(
        &self,
        task: TaskId,
    ) -> Option<zero_platform_tokio::contract::scope::TaskJoined> {
        let witness = self.join_witness()?;
        if witness.task_id() != Some(task) {
            return None;
        }
        let joined = witness.try_joined()?;
        self.task_failure.get()?.receive(&joined).ok()?;
        Some(joined)
    }
}

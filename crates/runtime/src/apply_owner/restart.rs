//! Reopen task admission only after the same configuration Scope really retired.
use super::*;
impl ApplyOwner {
    /// This retires no component/native resource. The runtime first confirms
    /// its original provider graph; this only reopens this task owner's admission.
    pub async fn reopen_after_retirement(&self) -> Result<(), String> {
        if !self.is_closing() {
            return Ok(());
        }
        let mut owner = self.owner.lock().await;
        let gate = self
            .commit_gate
            .lock()
            .expect("configuration commit gate poisoned");
        if !self.is_closing() {
            return Ok(());
        }
        owner.result()?;
        if owner.occupied
            || owner.registration.is_some()
            || owner
                .scope
                .as_ref()
                .is_some_and(|scope| !scope.registrar().retirement_observation().is_drained())
        {
            return Err("configuration original Scope retirement remains unconfirmed".into());
        }
        let retired = owner.scope.take();
        // A stale completion notification is not a new task or unconsumed join.
        // The original Scope census above proves every actual exit was consumed.
        self.completed.store(false, Ordering::Release);
        self.closing.store(false, Ordering::Release);
        drop(gate);
        drop(owner);
        drop(retired);
        Ok(())
    }
}

//! Same actual lifecycle receiver; no second fault book or resource authority.
use super::*;
use zero_platform_tokio::contract::{lifecycle::ResourceFailure, scope::TaskScopeIdentity};
impl HostResources {
    pub(in crate::component) fn protocol_failure_receiver(&self) -> Arc<DeclaredReceiver> {
        self.protocol_failures.clone()
    }
    pub(in crate::component) fn bind_protocol_supervision(
        &self,
        signal: Option<std::sync::Weak<crate::component::admission::HostAdmission>>,
    ) {
        if let Some(signal) = signal {
            assert!(
                self.protocol_supervision.set(signal).is_ok(),
                "one actual Host signal"
            );
        }
    }
    pub(super) fn receive_protocol_fault(
        &self,
        scope: &TaskScopeIdentity,
        source: &Arc<ChannelError>,
    ) -> Result<Arc<ResourceFailure>, ChannelError> {
        // Expired identity is never reconstructed into an owner/registrar.
        // Cleanup may occur after admission closes, so this does not test open.
        let original = self
            .original_tasks
            .as_ref()
            .and_then(|tasks| tasks.retirement_observation())
            .ok_or_else(|| self.denied())?;
        let actual = self.instance.ok_or_else(|| self.denied())?;
        if !scope.same_scope(&original.scope_identity()) {
            return Err(self.denied());
        }
        self.protocol_failures.original(actual, source.clone());
        let retained = self.protocol_failures.get().ok_or_else(|| self.denied())?;
        // All actual source/receipt storage is complete before any external
        // cancellation/waker callback. It neither closes nor ACKs an original.
        self.cancel.cancel();
        if let Some(signal) = self
            .protocol_supervision
            .get()
            .and_then(std::sync::Weak::upgrade)
        {
            signal.signal().notify_waiters();
        }
        Ok(retained)
    }
}

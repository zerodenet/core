//! A concrete original UDP response duty, never registrar recovery from a witness.
use super::*;
impl HostPeerBinding {
    pub(crate) fn datagram_receiver_registrar(
        &self,
        scope: &zero_platform_tokio::contract::scope::TaskScopeIdentity,
    ) -> Result<zero_platform_tokio::contract::scope::WeakTaskRegistrar, ChannelError> {
        if !scope.same_scope(&self.registrar.scope_identity())
            || !self.registrar.registration_open()
            || self.cancel.is_cancelled()
            || self
                .join_witness()
                .is_none_or(|original| original.is_joined())
        {
            return Err(self.error(Reason::Revoked, "actual UDP receiver Scope is not live"));
        }
        Ok(self.registrar.clone())
    }
}

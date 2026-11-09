//! Same Source receiver before any native accept or child registration.
use super::*;
impl InboundOwner {
    pub(in crate::runtime::inbound_owners) fn client_failure_receiver(
        &self,
    ) -> Option<&zero_platform_tokio::AcceptedTaskFailures> {
        self.client_task_failures.get()
    }
    pub(in crate::runtime::inbound_owners) fn prepare_client_failures(
        &self,
        originals: &zero_runtime::HostOriginalBudgets,
    ) -> Result<zero_platform_tokio::AcceptedTaskFailures, ChannelError> {
        let memory = originals.packet.driver_metadata_view();
        if self.client_task_failures.get().is_none() {
            let actual = zero_platform_tokio::AcceptedTaskFailures::prepare(
                self.identity,
                &self.registrar,
                &memory,
            )?;
            // A concurrent cold installer may win. Neither candidate owns IO,
            // tasks or acceptance, so the unused paid allocation can return.
            if let Err(unused) = self.client_task_failures.set(actual) {
                drop(unused);
            }
        }
        let actual = self.client_failure_receiver().expect("installed receiver");
        if !actual.matches(self.identity, &self.registrar.scope_identity(), &memory) {
            return Err(ChannelError::new(
                Stage::Prepare,
                Reason::StalePlan,
                Impact::Component,
            ));
        }
        Ok(actual.clone())
    }
}

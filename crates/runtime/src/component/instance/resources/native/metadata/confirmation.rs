use super::*;
use zero_platform_tokio::contract::scope::TaskRegistrar;

impl HostResources {
    pub(in crate::component::instance::resources::native) fn confirm_metadata_original_users(
        &self,
        ordinal: usize,
        receipt: &MetadataUsersClosedReceipt,
        tasks: &TaskRegistrar,
    ) -> Result<(), ChannelError> {
        // This provider belongs to the one actual captured instance. Private
        // issuer identity also prevents equal ordinal/task numbers in another
        // instance, generation, bind attempt, or provider from replaying a proof.
        let (instance, scope) = self
            .metadata
            .identity
            .as_ref()
            .ok_or_else(|| self.denied())?;
        if instance.component != self.component || !scope.matches(tasks) {
            return Err(self.denied());
        }
        let entry = self
            .metadata
            .entries
            .get(&ordinal)
            .ok_or_else(|| self.denied())?;
        let native = entry.lock().ok_or_else(|| {
            error(
                self.component,
                Reason::Capacity,
                "metadata users confirmation is busy",
            )
        })?;
        if !native.users.confirm(receipt, tasks) {
            return Err(self.denied());
        }
        Ok(())
    }
}

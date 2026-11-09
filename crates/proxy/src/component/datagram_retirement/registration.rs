//! Same application-owned logical controller; no native/donor close authority.
use super::*;
use zero_platform_tokio::contract::scope::TaskJoinWitness;
#[derive(Clone)]
pub(crate) struct DatagramRegistration {
    entry: Arc<Mutex<Entry>>,
    caller: TaskJoinWitness,
    owner: std::sync::Weak<RetiredDatagrams>,
    id: u64,
}
impl DatagramReservation {
    pub(crate) fn registration(
        &self,
        caller: &TaskJoinWitness,
    ) -> Result<DatagramRegistration, ChannelError> {
        if caller.is_joined() || !caller.scope_identity().resource_open() {
            return Err(admission_error(Reason::Revoked));
        }
        Ok(DatagramRegistration {
            entry: self.entry.clone(),
            caller: caller.clone(),
            owner: Arc::downgrade(&self.owner),
            id: self.id,
        })
    }
}
impl DatagramRegistration {
    pub(crate) async fn retire_and_wait(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<CloseReport, ChannelError> {
        let owner = self
            .owner
            .upgrade()
            .ok_or_else(|| admission_error(Reason::Revoked))?;
        owner.wait_one(self.id, &self.entry, deadline).await
    }
    pub(crate) fn same_entry(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.entry, &other.entry) && self.caller.same_registration(&other.caller)
    }
    pub(crate) fn belongs_to(&self, caller: &TaskJoinWitness) -> bool {
        self.caller.same_registration(caller)
    }
    pub(crate) fn closed(&self) -> bool {
        // Only the original poller publishes this after complete actual control
        // report and actual controller return; neither view Drop nor ACK does.
        self.entry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .closed
    }
}

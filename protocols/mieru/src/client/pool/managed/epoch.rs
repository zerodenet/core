//! Network/DNS changes retire originals before admitting a new acquisition.
use super::*;
type Epoch = (
    zero_core::component::NetworkGeneration,
    Option<zero_platform_tokio::contract::dns::DnsGeneration>,
);
impl ManagedState {
    pub(crate) fn user_for_epoch(self: &Arc<Self>, epoch: Epoch) -> io::Result<Option<UserLease>> {
        let mut state = self.state.lock().unwrap();
        if !state.ready
            || state.stopped
            || self.cancel.is_cancelled()
            || self.scope_cancel.is_cancelled()
        {
            return Err(io::ErrorKind::Interrupted.into());
        }
        if state.epoch.is_some_and(|old| old != epoch)
            && (!state.records.is_empty() || state.users != 0)
        {
            return Ok(None);
        }
        state.epoch = Some(epoch);
        state.users = state
            .users
            .checked_add(1)
            .ok_or_else(|| io::Error::other("Mieru user count exhausted"))?;
        Ok(Some(UserLease(self.clone())))
    }
    pub(crate) fn retire_epoch(&self) {
        let records = self.state.lock().unwrap().records.clone();
        for record in records {
            record.request_retirement();
        }
    }
}

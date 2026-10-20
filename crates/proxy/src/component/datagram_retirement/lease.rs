use super::{entry::Entry, RetiredDatagrams};
use std::sync::{Arc, Mutex};
use zero_platform_tokio::contract::{
    lifecycle::{CloseReport, DynChannelControl},
    BoxFuture, ChannelError, CloseReason,
};
pub(crate) struct DatagramReservation {
    owner: Arc<RetiredDatagrams>,
    id: u64,
    entry: Arc<Mutex<Entry>>,
    adopted: bool,
}
impl DatagramReservation {
    pub(super) fn new(owner: Arc<RetiredDatagrams>, id: u64, entry: Arc<Mutex<Entry>>) -> Self {
        Self {
            owner,
            id,
            entry,
            adopted: false,
        }
    }
    /// The actual controller enters its pre-reserved persistent owner before
    /// this reference-only lease can be exposed to a business consumer.
    pub(crate) fn adopt(
        mut self,
        control: Box<dyn DynChannelControl>,
    ) -> Box<dyn DynChannelControl> {
        self.owner.adopt(self.id, &self.entry, control);
        self.adopted = true;
        Box::new(Lease {
            owner: self.owner.clone(),
            id: self.id,
            entry: self.entry.clone(),
        })
    }
}
impl Drop for DatagramReservation {
    fn drop(&mut self) {
        if !self.adopted {
            self.owner.abandon_reservation(self.id);
        }
    }
}
struct Lease {
    owner: Arc<RetiredDatagrams>,
    id: u64,
    entry: Arc<Mutex<Entry>>,
}
impl DynChannelControl for Lease {
    fn cancel(&self, reason: CloseReason) {
        self.owner.retire(self.id, &self.entry, reason);
    }
    fn wait_closed(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> BoxFuture<'_, Result<CloseReport, ChannelError>> {
        Box::pin(self.owner.wait_one(self.id, &self.entry, deadline))
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        self.cancel(CloseReason::Shutdown);
    }
}

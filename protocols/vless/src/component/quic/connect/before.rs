//! No actual registrar has been called at this stage. A foreign receiver panic
//! must not turn this known no-task state into an uncertain self-wait.
use super::*;
pub(super) struct BeforeActor(pub Option<PacketMetadataArc<Entry>>);
impl BeforeActor {
    pub fn disarm(&mut self) {
        drop(self.0.take());
    }
}
impl Drop for BeforeActor {
    fn drop(&mut self) {
        if let Some(original) = self.0.take() {
            original.owners.set_actor(None);
            original.retirement.request_retirement();
        }
    }
}

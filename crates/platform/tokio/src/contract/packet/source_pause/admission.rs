//! New opens only. Existing transferred IO does not observe this pause gate.
use super::*;
pub struct PacketSourceAcquisition {
    original: Arc<PacketSourcePause>,
}
impl Drop for PacketSourceAcquisition {
    fn drop(&mut self) {
        let mut state = self.original.state.lock().unwrap();
        state.acquiring -= 1;
        drop(state);
        self.original.changed.notify_waiters();
    }
}
impl PacketSourcePause {
    pub fn try_enter(self: &Arc<Self>) -> Result<PacketSourceAcquisition, ChannelError> {
        let mut state = self.state.lock().unwrap();
        if !self.live(&state) {
            return Err(pause_error(Reason::Revoked));
        }
        if state.request.is_some() {
            return Err(
                pause_error(Reason::Capacity).with_source(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    "actual source is pausing new acquisitions",
                )),
            );
        }
        state.acquiring = state
            .acquiring
            .checked_add(1)
            .ok_or_else(|| pause_error(Reason::Capacity))?;
        Ok(PacketSourceAcquisition {
            original: self.clone(),
        })
    }
}

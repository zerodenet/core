//! One concrete control workspace supplied by the actual existing caller for
//! this operation. No buffer pool, callback allocator, task or IO is created.
use super::*;
impl<R: RngCore + Send> Tunn<R> {
    /// Production ownership mode persists for this engine's actual lifetime.
    /// It never changes back to a library allocation path after a failed call.
    pub fn require_owned_control(&mut self) {
        self.owned_control = true;
    }
    pub fn install_owned_control(&mut self, packet: Packet) -> Result<(), Packet> {
        if self.control_workspace.is_some()
            || !packet.has_owned_custody()
            || packet.len() < std::mem::size_of::<WgHandshakeInit>()
        {
            return Err(packet);
        }
        self.owned_control = true;
        self.control_workspace = Some(packet);
        Ok(())
    }
    pub fn take_unused_owned_control(&mut self) -> Option<Packet> {
        self.control_workspace.take()
    }
    pub(super) fn take_owned_control_packet(
        &mut self,
        length: usize,
    ) -> Result<Packet, WireGuardError> {
        let Some(mut packet) = self.control_workspace.take() else {
            return Err(WireGuardError::DestinationBufferTooSmall);
        };
        if packet.len() < length {
            self.control_workspace = Some(packet);
            return Err(WireGuardError::DestinationBufferTooSmall);
        }
        packet.truncate(length);
        Ok(packet)
    }
}

//! The original host seals its one actual Source admission gate before IO.
use super::*;
use crate::contract::{
    packet::{PacketSourceAcquisition, PacketSourceCheckpoint},
    ChannelError, Impact, Reason, Stage,
};
impl NativeDatagramOrigin {
    pub fn bind_packet_source(
        &self,
        checkpoint: &PacketSourceCheckpoint,
    ) -> Result<(), ChannelError> {
        let source = checkpoint
            .bind_origin(self.owner.instance(), self.owner.scope(), &self.driver)
            .ok_or_else(|| failure(Reason::StalePlan))?;
        let actual = self.owner.0.source_pause.get_or_init(|| source.clone());
        if !std::sync::Weak::ptr_eq(actual, &source) {
            return Err(failure(Reason::StalePlan));
        }
        Ok(())
    }
    pub fn try_enter_packet_source(&self) -> Result<PacketSourceAcquisition, ChannelError> {
        if self.is_retired() {
            return Err(failure(Reason::Revoked));
        }
        let source = self
            .owner
            .0
            .source_pause
            .get()
            .and_then(std::sync::Weak::upgrade)
            .ok_or_else(|| failure(Reason::Revoked))?;
        source.try_enter()
    }
}
fn failure(reason: Reason) -> ChannelError {
    ChannelError::new(Stage::Prepare, reason, Impact::Component)
}

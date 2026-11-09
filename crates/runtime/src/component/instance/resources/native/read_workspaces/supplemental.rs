//! Actual pre-bind receive workers and this wire's separate pending TX copy.
use super::*;
use zero_tun::packet_memory::{
    NativeReadGrant, PacketDatagramWorkspaces, PacketMetadataCredit, PacketMetadataLayout,
    PacketReadBatch,
};
mod flags;
pub(super) use flags::ReceivedFlags;
pub(super) struct OriginalSupplemental {
    receive: PacketReadBatch,
    copy: Option<NativeReadGrant>,
    target: Option<NativeReadGrant>,
}
impl OriginalSupplemental {
    pub(super) fn revoke(&self) {
        for grant in self.receive[2..]
            .iter()
            .chain(self.copy.iter())
            .chain(self.target.iter())
        {
            grant.revoke();
        }
    }
}
impl HostPacketReadReservation {
    pub(super) fn allocate_original_workspaces(
        &self,
    ) -> Result<
        (
            PacketDatagramWorkspaces,
            OriginalSupplemental,
            ReceivedFlags,
        ),
        ChannelError,
    > {
        let count = self
            .lengths
            .len()
            .checked_add(self.supplemental.receive_workspaces.len())
            .ok_or_else(|| self.denied())?;
        let receive_layout = PacketMetadataLayout::array::<usize>(count)
            .map_err(|source| self.denied().with_source(source))?;
        let receive_metadata = self
            .roots
            .packet
            .reserve_driver_metadata(receive_layout)
            .map_err(|source| self.denied().with_source(source))?;
        let mut receive = Box::<[usize]>::new_uninit_slice(count);
        for (target, value) in receive.iter_mut().zip(
            self.lengths
                .iter()
                .chain(self.supplemental.receive_workspaces.iter()),
        ) {
            target.write(value.get());
        }
        let receive = unsafe { receive.assume_init() };
        let mut transmit = [0; 3];
        let mut sending = 0;
        for length in self
            .control
            .iter()
            .chain(self.supplemental.transmit_copy.iter())
            .chain(self.supplemental.transmit_target.iter())
        {
            transmit[sending] = length.get();
            sending += 1;
        }
        let received = ReceivedFlags::prepare(
            &self.roots.packet,
            self.supplemental.receive_workspaces.len(),
        )
        .map_err(|source| self.denied().with_source(source))?;
        // One transaction before the original descriptor's first native bind.
        // Either every actual buffer promise is paid or none can escape.
        let grants = self
            .roots
            .packet
            .owned_io_workspaces(&receive, &transmit[..sending])
            .map_err(|source| {
                let mut failure = error(
                    self.instance.component,
                    Reason::Capacity,
                    "same UDP original cannot prepay all actual read/control/copy workspaces",
                );
                failure.delivery = zero_platform_tokio::contract::DeliveryState::NotSubmitted;
                failure.with_source(source)
            })?;
        drop(receive); // actual transient declaration array frees first
        drop(receive_metadata);
        let mut transmit = grants.transmit.iter();
        let original = PacketDatagramWorkspaces {
            reads: [grants.reads[0].clone(), grants.reads[1].clone()],
            control: self
                .control
                .map(|_| transmit.next().expect("declared control").clone()),
        };
        let supplemental = OriginalSupplemental {
            receive: grants.reads,
            copy: self
                .supplemental
                .transmit_copy
                .map(|_| transmit.next().expect("declared wire copy").clone()),
            target: self
                .supplemental
                .transmit_target
                .map(|_| transmit.next().expect("declared target backing").clone()),
        };
        Ok((original, supplemental, received))
    }
    /// Unique transfer to an existing declared reader, not a new IO/Task grant.
    pub fn take_supplemental_receive(
        &self,
        reader: usize,
    ) -> Result<NativeReadGrant, ChannelError> {
        if !self.scope.resource_open() {
            return Err(self.denied());
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.stopped || !state.taken || state.received.get(reader) != Some(&false) {
            return Err(self.denied());
        }
        let grant = state
            .supplemental
            .as_ref()
            .and_then(|original| original.receive.get(reader + 2))
            .cloned()
            .ok_or_else(|| self.denied())?;
        state.received[reader] = true;
        Ok(grant)
    }
    pub fn send_target_workspace(&self) -> Result<Option<NativeReadGrant>, ChannelError> {
        if !self.scope.resource_open() {
            return Err(self.denied());
        }
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.stopped || !state.taken {
            return Err(self.denied());
        }
        Ok(state
            .supplemental
            .as_ref()
            .ok_or_else(|| self.denied())?
            .target
            .clone())
    }
    /// Read-only clone of this same prepaid wire copy. Its unique buffer can
    /// only be taken once at a time; consumers cannot install Root policy.
    pub fn send_copy_workspace(&self) -> Result<Option<NativeReadGrant>, ChannelError> {
        if !self.scope.resource_open() {
            return Err(self.denied());
        }
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.stopped || !state.taken {
            return Err(self.denied());
        }
        Ok(state
            .supplemental
            .as_ref()
            .ok_or_else(|| self.denied())?
            .copy
            .clone())
    }
}

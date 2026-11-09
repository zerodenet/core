//! Actual pre-bind receive workers and this wire's separate pending TX copy.
use super::*;
use zero_tun::packet_memory::{NativeReadGrant, PacketDatagramWorkspaces};
pub(super) struct OriginalSupplemental {
    receive: Vec<NativeReadGrant>,
    copy: Option<NativeReadGrant>,
    target: Option<NativeReadGrant>,
}
impl OriginalSupplemental {
    pub(super) fn revoke(&self) {
        for grant in self
            .receive
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
    ) -> Result<(PacketDatagramWorkspaces, OriginalSupplemental), ChannelError> {
        let receive = self
            .lengths
            .iter()
            .chain(self.supplemental.receive_workspaces.iter())
            .map(|length| length.get())
            .collect::<Vec<_>>();
        let transmit = self
            .control
            .iter()
            .chain(self.supplemental.transmit_copy.iter())
            .chain(self.supplemental.transmit_target.iter())
            .map(|length| length.get())
            .collect::<Vec<_>>();
        // One transaction before the original descriptor's first native bind.
        // Either every actual buffer promise is paid or none can escape.
        let grants = self
            .roots
            .packet
            .io_workspaces(&receive, &transmit)
            .map_err(|source| {
                let mut failure = error(
                    self.instance.component,
                    Reason::Capacity,
                    "same UDP original cannot prepay all actual read/control/copy workspaces",
                );
                failure.delivery = zero_platform_tokio::contract::DeliveryState::NotSubmitted;
                failure.with_source(source)
            })?;
        let mut reads = grants.reads.into_iter();
        let mut transmit = grants.transmit.into_iter();
        let original = PacketDatagramWorkspaces {
            reads: [
                reads.next().expect("declared read0"),
                reads.next().expect("declared read1"),
            ],
            control: self
                .control
                .map(|_| transmit.next().expect("declared control")),
        };
        let supplemental = OriginalSupplemental {
            receive: reads.collect(),
            copy: self
                .supplemental
                .transmit_copy
                .map(|_| transmit.next().expect("declared wire copy")),
            target: self
                .supplemental
                .transmit_target
                .map(|_| transmit.next().expect("declared target backing")),
        };
        Ok((original, supplemental))
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
            .and_then(|original| original.receive.get(reader))
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

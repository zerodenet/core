//! Cold memory only. Actual driver/IO admission remains a later, separate gate.
use super::*;
use zero_platform_tokio::contract::component::{
    GrantedLogicalPacketWorkspaces, PacketSupplementalMemory,
};
use zero_tun::packet_memory::{PacketMetadataArc, PacketMetadataLayout};
impl HostPacketReadReservation {
    pub(in crate::component::instance::resources::native) fn prepare_software(
        instance: ComponentInstanceId,
        scope: TaskScopeIdentity,
        roots: Arc<crate::HostOriginalBudgets>,
        lengths: [NonZeroUsize; 2],
        control: Option<NonZeroUsize>,
        supplemental: &PacketSupplementalMemory,
    ) -> Result<PacketMetadataArc<Self>, ChannelError> {
        let denied = |source| {
            error(
                instance.component,
                Reason::Capacity,
                "software Packet memory cannot fit",
            )
            .with_source(source)
        };
        let header = roots
            .packet
            .reserve_driver_metadata(PacketMetadataArc::<Self>::layout().map_err(denied)?)
            .map_err(denied)?;
        let description = roots
            .packet
            .reserve_driver_metadata(
                PacketMetadataLayout::array::<NonZeroUsize>(supplemental.receive_workspaces.len())
                    .map_err(denied)?,
            )
            .map_err(denied)?;
        // Scope may still be suspended: this is pure allocation prepayment,
        // not registration/IO authority or an invented Source task.
        let mut original = Self {
            instance,
            scope,
            lengths,
            control,
            roots,
            supplemental: supplemental.clone(),
            state: Mutex::new(ReadState::default()),
            _description_credit: Some(description),
        };
        let (grants, supplemental, received) = original.allocate_original_workspaces()?;
        let state = original
            .state
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.grants = Some(grants);
        state.supplemental = Some(supplemental);
        state.received = received;
        PacketMetadataArc::from_prepaid(original, header)
            .map_err(|(source, _original, _header)| denied(source))
    }
    pub(in crate::component::instance::resources::native) fn software_driver(
        &self,
    ) -> Option<TaskJoinWitness> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.taken.then(|| state.driver.clone()).flatten()
    }
    pub(in crate::component::instance::resources::native) fn software_send(
        &self,
    ) -> Result<
        (
            TaskJoinWitness,
            zero_tun::packet_memory::NativeReadGrant,
            zero_tun::packet_memory::NativeReadGrant,
        ),
        ChannelError,
    > {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.stopped || !state.taken {
            return Err(self.denied());
        }
        let memory = state.supplemental.as_ref().ok_or_else(|| self.denied())?;
        Ok((
            state.driver.clone().ok_or_else(|| self.denied())?,
            memory.copy.clone().ok_or_else(|| self.denied())?,
            memory.target.clone().ok_or_else(|| self.denied())?,
        ))
    }
    pub(in crate::component::instance::resources::native) fn take_software(
        &self,
        driver: &TaskJoinWitness,
    ) -> Result<GrantedLogicalPacketWorkspaces, ChannelError> {
        if driver.task_id().is_none() {
            return Err(self.denied());
        }
        self.prepare_for_driver(driver)?;
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.stopped || state.taken {
            return Err(self.denied());
        }
        let grants = state.grants.as_ref().ok_or_else(|| self.denied())?;
        let supplemental = state.supplemental.as_ref().ok_or_else(|| self.denied())?;
        let result = GrantedLogicalPacketWorkspaces {
            reads: grants.reads.clone(),
            control: grants.control.clone(),
            receive: supplemental.receive.clone(),
            transmit_copy: supplemental.copy.clone(),
            transmit_target: supplemental.target.clone(),
        };
        state.taken = true;
        state.control_taken = true;
        state.received.fill(true);
        Ok(result)
    }
    pub(in crate::component::instance::resources::native) fn software_released(&self) -> bool {
        self.released()
    }
}

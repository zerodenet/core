//! The exact same original Source has a single pair of prepaid read workspaces.
use super::*;
impl NativeDatagramGrant {
    #[cfg(feature = "raw-ip-runtime")]
    pub(in crate::runtime::inbound_owners) fn take_packet_wire_launch(
        &self,
        datagram: usize,
    ) -> Result<zero_platform_tokio::contract::scope::TaskLaunch, ChannelError> {
        if datagram != 0 || self.cancel.is_cancelled() || !self.bound.load(Ordering::Acquire) {
            return Err(ChannelError::new(
                Stage::Prepare,
                Reason::Revoked,
                Impact::Component,
            ));
        }
        self.packet_wire_launch
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| {
                ChannelError::new(Stage::Prepare, Reason::InvalidConfig, Impact::Component)
            })
    }

    #[cfg(feature = "raw-ip-runtime")]
    pub(in crate::runtime::inbound_owners) fn install_packet_memory(
        &self,
        memory: zero_platform_tokio::contract::packet::PacketSourceMemory,
    ) -> Result<(), ChannelError> {
        let expected = self
            .packet_reads
            .as_ref()
            .ok_or_else(|| {
                ChannelError::new(Stage::Prepare, Reason::InvalidConfig, Impact::Component)
            })?
            .packet_memory_view()?;
        if self.cancel.is_cancelled() || !memory.view.same_original(&expected) {
            return Err(ChannelError::new(
                Stage::Prepare,
                Reason::StalePlan,
                Impact::Component,
            ));
        }
        if let Some(previous) = self.packet_memory.get() {
            return if previous.layout == memory.layout && previous.view.same_original(&memory.view)
            {
                Ok(())
            } else {
                Err(ChannelError::new(
                    Stage::Prepare,
                    Reason::StalePlan,
                    Impact::Component,
                ))
            };
        }
        self.packet_memory
            .set(memory)
            .map_err(|_| ChannelError::new(Stage::Prepare, Reason::StalePlan, Impact::Component))
    }
    #[cfg(feature = "raw-ip-runtime")]
    pub(in crate::runtime::inbound_owners) fn packet_memory(
        &self,
    ) -> Option<zero_platform_tokio::contract::packet::PacketSourceMemory> {
        if self.cancel.is_cancelled() || self.original_origin().is_none() {
            return None;
        }
        let expected = self.packet_reads.as_ref()?.packet_memory_view().ok()?;
        self.packet_memory
            .get()
            .filter(|memory| memory.view.same_original(&expected))
            .cloned()
    }

    pub(in crate::runtime::inbound_owners::bind) fn with_packet_reads(
        self,
        declarations: &[ResourceDeclaration],
    ) -> Result<Self, ChannelError> {
        #[cfg(feature = "raw-ip-runtime")]
        let mut owned = self;
        #[cfg(not(feature = "raw-ip-runtime"))]
        let owned = self;
        let reads = crate::component::instance::packet_datagram_read_spec(declarations, 0)?;
        #[cfg(feature = "raw-ip-runtime")]
        if let Some(reads) = reads {
            let roots = owned.originals.clone().ok_or_else(|| {
                ChannelError::new(Stage::Prepare, Reason::InvalidConfig, Impact::Component)
            })?;
            let tasks = owned.scope.as_ref().ok_or_else(|| {
                ChannelError::new(Stage::Prepare, Reason::InvalidConfig, Impact::Component)
            })?;
            // InboundOwner has already reserved its own Main. Reserve only
            // this actual additional Critical wire, from the same local and
            // Host-global registrar budgets, before the first physical bind.
            let wire = tasks
                .reserve_launch(zero_platform_tokio::contract::scope::TaskClass::Critical)
                .map_err(|source| {
                    ChannelError::new(Stage::Prepare, Reason::Capacity, Impact::Component)
                        .with_source(source)
                })?;
            *owned.packet_wire_launch.lock().unwrap() = Some(wire);
            owned.packet_reads = Some(
                crate::component::instance::HostPacketReadReservation::new_with_supplemental(
                    owned.identity,
                    tasks.scope_identity(),
                    roots,
                    reads,
                    crate::component::instance::packet_datagram_control_spec(declarations, 0)?,
                    crate::component::instance::packet_datagram_supplemental_spec(declarations, 0)?,
                )?,
            );
        }
        #[cfg(not(feature = "raw-ip-runtime"))]
        if reads.is_some() {
            return Err(ChannelError::new(
                Stage::Prepare,
                Reason::Unsupported,
                Impact::Component,
            ));
        }
        Ok(owned)
    }
    pub(in crate::runtime::inbound_owners::bind) fn needs_packet_reads(&self) -> bool {
        #[cfg(feature = "raw-ip-runtime")]
        {
            self.packet_reads.is_some()
        }
        #[cfg(not(feature = "raw-ip-runtime"))]
        false
    }
    #[cfg(feature = "raw-ip-runtime")]
    pub(in crate::runtime::inbound_owners) fn take_packet_receive_workspace(
        &self,
        datagram: usize,
        reader: usize,
    ) -> Result<zero_tun::packet_memory::NativeReadGrant, ChannelError> {
        if datagram != 0 || self.cancel.is_cancelled() {
            return Err(ChannelError::new(
                Stage::Prepare,
                Reason::Revoked,
                Impact::Component,
            ));
        }
        self.packet_reads
            .as_ref()
            .ok_or_else(|| {
                ChannelError::new(Stage::Prepare, Reason::InvalidConfig, Impact::Component)
            })?
            .take_supplemental_receive(reader)
    }
    #[cfg(feature = "raw-ip-runtime")]
    pub(in crate::runtime::inbound_owners) fn take_packet_control(
        &self,
        datagram: usize,
    ) -> Result<Option<zero_tun::packet_memory::NativeReadGrant>, ChannelError> {
        if datagram != 0 || self.cancel.is_cancelled() {
            return Err(ChannelError::new(
                Stage::Prepare,
                Reason::Revoked,
                Impact::Component,
            ));
        }
        self.packet_reads
            .as_ref()
            .ok_or_else(|| {
                ChannelError::new(Stage::Prepare, Reason::InvalidConfig, Impact::Component)
            })?
            .take_control()
    }
    #[cfg(feature = "raw-ip-runtime")]
    pub(in crate::runtime::inbound_owners) fn take_packet_reads(
        &self,
        datagram: usize,
    ) -> Result<[zero_tun::packet_memory::NativeReadGrant; 2], ChannelError> {
        if datagram != 0 || self.cancel.is_cancelled() {
            return Err(ChannelError::new(
                Stage::Prepare,
                Reason::Revoked,
                Impact::Component,
            ));
        }
        self.packet_reads
            .as_ref()
            .ok_or_else(|| {
                ChannelError::new(Stage::Prepare, Reason::InvalidConfig, Impact::Component)
            })?
            .take()
    }
}

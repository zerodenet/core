//! The exact same original Source has a single pair of prepaid read workspaces.
use super::*;
impl NativeDatagramGrant {
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
            owned.packet_reads = Some(crate::component::instance::HostPacketReadReservation::new(
                owned.identity,
                tasks.scope_identity(),
                roots,
                reads,
            )?);
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

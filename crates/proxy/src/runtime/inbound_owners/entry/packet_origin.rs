//! Read the same already-retained Source observer cell; no new weak owner.
use super::*;
impl InboundOwner {
    pub(in crate::runtime) fn packet_carrier_origin(
        &self,
    ) -> Result<zero_platform_tokio::controlled_carrier::CarrierOrigin, ChannelError> {
        self.packet_native
            .get()
            .and_then(std::sync::Weak::upgrade)
            .and_then(|grant| grant.packet_source_origin())
            .ok_or_else(|| ChannelError::new(Stage::Prepare, Reason::Revoked, Impact::Component))
    }
}
impl InboundTaskAccess {
    pub(in crate::runtime) fn packet_carrier_origin(
        &self,
    ) -> Result<zero_platform_tokio::controlled_carrier::CarrierOrigin, ChannelError> {
        self.packet_native
            .get()
            .and_then(std::sync::Weak::upgrade)
            .and_then(|grant| grant.packet_source_origin())
            .ok_or_else(|| ChannelError::new(Stage::Prepare, Reason::Revoked, Impact::Component))
    }
}

impl InboundOwner {
    pub(in crate::runtime::inbound_owners) fn validate_packet_software_activation(
        &self,
    ) -> Result<(), ChannelError> {
        self.result().map_err(|source| {
            ChannelError::new(Stage::Prepare, Reason::Io, Impact::Component).with_source(source)
        })?;
        if self.is_ready()
            || self.driver_finished.load(Ordering::Acquire)
            || self.cancel.is_cancelled()
        {
            return Err(ChannelError::new(
                Stage::Prepare,
                Reason::Revoked,
                Impact::Component,
            ));
        }
        Ok(())
    }
}

impl InboundOwner {
    /// Same actual Source registrations only; neither publication nor Ready.
    pub(in crate::runtime::inbound_owners) fn matches_packet_source_registration(
        &self,
        native: &zero_platform_tokio::controlled_datagram::provider::NativeDatagramOrigin,
    ) -> bool {
        self.main_registration.witness.get().is_some_and(|main| {
            !self.cancel.is_cancelled()
                && !main.is_joined()
                && !native.driver().is_joined()
                && native.owner().instance() == self.identity
                && native
                    .driver()
                    .scope_identity()
                    .same_scope(&main.scope_identity())
                && main
                    .scope_identity()
                    .same_scope(&self.registrar.scope_identity())
                && main.scope_identity().resource_open()
        })
    }
}

impl InboundOwner {
    pub(in crate::runtime::inbound_owners) fn receive_packet_driver_failure(
        &self,
        native: &Arc<super::super::bind::native::udp::NativeDatagramGrant>,
        grant: &zero_platform_tokio::contract::component::GrantedPacketServiceCaller,
        source: Arc<ChannelError>,
    ) -> Result<bool, ChannelError> {
        let denied = || ChannelError::new(Stage::Close, Reason::StalePlan, Impact::Component);
        if !self
            .packet_native
            .get()
            .and_then(std::sync::Weak::upgrade)
            .is_some_and(|same| Arc::ptr_eq(&same, native))
            || self.identity != grant.original().instance()
            || !self
                .registrar
                .scope_identity()
                .same_scope(&grant.caller().scope_identity())
            || !native.matches_retained_packet_driver(grant)
            || !grant.matches_cleanup_failure(&source)
        {
            return Err(denied());
        }
        // NativeBindGrant::Datagram has no extra native Resource rows. ICMP
        // and source lifecycle use their own identities/flags. Ambiguity is
        // rejected rather than receiving an error on an arbitrary controller.
        let receiver = {
            let rows = self
                .resources
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut matches = rows
                .iter()
                .filter(|row| row.identity == self.identity && row.native_on_driver_exit);
            let actual = matches.next().cloned().ok_or_else(denied)?;
            if matches.next().is_some() {
                return Err(denied());
            }
            actual
        };
        // Exact persistence result, not an old canonical Arc or a preexisting
        // acknowledged bit. The center helper owns child-receipt handoff.
        Ok(receiver
            .cleanup_failures
            .try_receive(source, cleanup_fault::Phase::Driver))
    }
}

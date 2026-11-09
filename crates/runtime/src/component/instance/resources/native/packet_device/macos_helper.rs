use super::*;
impl PacketDeviceOwner {
    pub(in crate::component::instance::resources::native) fn prepare_macos_helper(
        &self,
        output_limit: usize,
        request_capacity: usize,
    ) -> Result<
        (
            PacketDeviceIdentity,
            zero_tun::MacosHelperAccess,
            Option<TaskJoinWitness>,
        ),
        ChannelError,
    > {
        self.memory
            .reserve_macos_job(&self.original, &self.identity, self.ordinal)?;
        let access = self.memory.prepare_macos_helper(
            &self.original,
            &self.binding,
            self.identity.instance().component,
            output_limit,
            request_capacity,
        )?;
        Ok((self.identity.clone(), access, self.memory.helper_witness()))
    }
    pub(in crate::component::instance::resources::native) fn stop_macos_helper(&self) {
        self.original.stop_macos();
    }
    pub(in crate::component::instance::resources::native) fn helper_matches(
        &self,
        identity: &PacketDeviceIdentity,
    ) -> bool {
        self.identity.same_provider(identity)
    }
    pub(in crate::component::instance::resources::native) fn accept_helper_failure(
        &self,
        witness: &TaskJoinWitness,
        source: Arc<ChannelError>,
    ) -> bool {
        self.memory
            .accept_helper_failure(&self.original, witness, source)
    }
    pub(in crate::component::instance::resources::native) fn helper_returned(
        &self,
        witness: &TaskJoinWitness,
    ) -> bool {
        self.memory.helper_returned(&self.original, witness)
    }
}

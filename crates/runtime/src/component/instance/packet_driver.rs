use super::*;
impl ComponentSlot {
    pub(in crate::component) fn matches_packet_driver_grant(
        &self,
        grant: &zero_platform_tokio::contract::component::GrantedPacketServiceCaller,
    ) -> bool {
        self.live
            .get()
            .and_then(|live| live.as_ref().ok())
            .is_some_and(|live| {
                let actual = matches!(live.started.get(), Some(Ok(())))
                    || live.started.get().is_none() && live.resources.packet_driver_start_open();
                actual && live.resources.matches_packet_driver_grant(grant)
            })
    }
}

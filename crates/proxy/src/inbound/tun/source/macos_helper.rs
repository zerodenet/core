//! The actual retained Source/provider, not a helper reconstructed from an id.
use super::*;
pub(super) struct Helper {
    identity: PacketDeviceIdentity,
    access: zero_tun::MacosHelperAccess,
    driver: Option<TaskJoinWitness>,
    resources: std::sync::Weak<dyn ResourceAccess>,
}
impl SystemTunSourceState {
    pub(in crate::inbound::tun) fn install_macos_helper(
        &self,
        prepared: (
            PacketDeviceIdentity,
            zero_tun::MacosHelperAccess,
            Option<TaskJoinWitness>,
            std::sync::Weak<dyn ResourceAccess>,
        ),
    ) -> Result<(), ChannelError> {
        let (identity, access, driver, resources) = prepared;
        if driver.as_ref().is_some_and(|driver| {
            !driver.scope_identity().same_scope(identity.scope()) || driver.is_joined()
        }) {
            return Err(error(Stage::Prepare, Reason::StalePlan));
        }
        self.helper
            .set(Helper {
                identity,
                access,
                driver,
                resources,
            })
            .map_err(|_| error(Stage::Prepare, Reason::StalePlan))
    }
    pub(super) fn validate_macos_helper(
        &self,
        instance: ComponentInstanceId,
        scope: &TaskScopeIdentity,
        resources: &Arc<dyn ResourceAccess>,
    ) -> Result<(), ChannelError> {
        let helper = self
            .helper
            .get()
            .ok_or_else(|| error(Stage::Prepare, Reason::Internal))?;
        if helper.identity.instance() != instance
            || !helper.identity.scope().same_scope(scope)
            || helper
                .resources
                .upgrade()
                .is_none_or(|actual| !Arc::ptr_eq(&actual, resources))
        {
            return Err(error(Stage::Prepare, Reason::StalePlan));
        }
        Ok(())
    }
    pub(in crate::inbound::tun) fn macos_helper_lease(
        &self,
    ) -> Result<zero_tun::MacosHelperLease, ChannelError> {
        self.helper
            .get()
            .ok_or_else(|| error(Stage::Prepare, Reason::Internal))?
            .access
            .lease_routes()
            .map_err(|source| error(Stage::Prepare, Reason::Io).with_source(source))
    }
    pub(in crate::inbound::tun) fn macos_system_dns_servers(
        &self,
    ) -> std::io::Result<Vec<std::net::IpAddr>> {
        self.helper
            .get()
            .ok_or_else(|| std::io::Error::other("actual Source helper missing"))?
            .access
            .system_dns_servers()
    }
    pub(in crate::inbound::tun) async fn wait_macos_helper_actor_returned(
        &self,
    ) -> Result<(), ChannelError> {
        let helper = self
            .helper
            .get()
            .ok_or_else(|| error(Stage::Close, Reason::Internal))?;
        let Some(driver) = helper.driver.as_ref() else {
            return Ok(());
        }; // Actual binding has no dedicated worker.
        let joined = driver.wait().await;
        let resources = helper
            .resources
            .upgrade()
            .ok_or_else(|| error(Stage::Close, Reason::Revoked))?;
        if let Err(source) = joined.result() {
            if !resources.receive_packet_device_helper_failure(&helper.identity, driver, source)? {
                return Err(error(Stage::Close, Reason::Internal));
            }
        }
        driver.wait_registration_released().await;
        if !resources.packet_device_helper_retired(&helper.identity, driver) {
            return Err(error(Stage::Close, Reason::Revoked));
        }
        Ok(())
    }
}

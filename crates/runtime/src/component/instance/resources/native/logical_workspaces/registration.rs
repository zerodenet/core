//! Complete actual wire membership, before the first opaque outer acquisition.
use super::*;
use zero_platform_tokio::{
    contract::component::PacketDeviceIdentity,
    controlled_carrier::LogicalPacketProviderRegistration,
};
impl LogicalWorkspaces {
    pub(in crate::component::instance::resources::native) fn register(
        &self,
        host: &HostResources,
        drivers: &[TaskJoinWitness],
    ) -> Result<LogicalPacketProviderRegistration, ChannelError> {
        let _taking = self.enter(host.component)?;
        let tasks = host.original_tasks.as_ref().ok_or_else(|| host.denied())?;
        let instance = host.instance.ok_or_else(|| host.denied())?;
        if drivers.is_empty()
            || drivers.len() != self.entries.len()
            || !tasks.registration_open()
            || drivers.iter().enumerate().any(|(index, driver)| {
                driver.task_id().is_none()
                    || driver.is_joined()
                    || !driver.scope_identity().same_scope(&tasks.scope_identity())
                    || drivers[..index]
                        .iter()
                        .any(|other| other.same_registration(driver))
                    || !self.entries.iter().any(|entry| {
                        entry
                            .original
                            .software_driver()
                            .is_some_and(|actual| actual.same_registration(driver))
                    })
            })
        {
            return Err(host.denied());
        }
        // Validate the actual predeclared send pair before any Source N is
        // reserved; absence cannot create a charged unusable logical source.
        if self
            .entries
            .iter()
            .any(|entry| entry.original.software_send().is_err())
        {
            return Err(host.denied());
        }
        if let Some(actual) = self.source.get() {
            if !(!actual.is_retired()
                && actual.drivers().len() == drivers.len()
                && actual.drivers().iter().all(|actual| {
                    drivers
                        .iter()
                        .any(|driver| driver.same_registration(actual))
                }))
            {
                return Err(host.denied());
            }
            host.prepare_packet_driver_calls(actual)?;
            self.install_copies(host, actual)?;
            return Ok(actual.clone());
        }
        let actual = LogicalPacketProviderRegistration::from_registered_drivers(
            PacketDeviceIdentity::new(instance, tasks.scope_identity()),
            drivers.to_vec(),
            host.cancel.child_token(),
        )?;
        // Existing SourceCustody and original N: software Scope completion
        // never proves that a separately acquired physical ancestor closed.
        actual.reserve_original_totals(tasks, &host.originals.carrier)?;
        self.source.set(actual.clone()).map_err(|_| host.denied())?;
        // If this later step rejects, the same Source/N remains strongly held
        // for recovery; another call retries it, never makes a second source.
        host.prepare_packet_driver_calls(&actual)?;
        self.install_copies(host, &actual)?;
        Ok(actual)
    }
    fn install_copies(
        &self,
        host: &HostResources,
        source: &LogicalPacketProviderRegistration,
    ) -> Result<(), ChannelError> {
        if self.copies_installed.get().is_some() {
            return Ok(());
        }
        let copies = self
            .entries
            .iter()
            .map(|entry| entry.original.software_send())
            .collect::<Result<Vec<_>, _>>()?;
        host.packet_calls
            .get()
            .ok_or_else(|| host.denied())?
            .install_send_workspaces(
                &zero_platform_tokio::controlled_carrier::CarrierOrigin::from_logical_packet(
                    source.clone(),
                ),
                copies,
            )?;
        self.copies_installed.set(()).map_err(|_| host.denied())
    }
}

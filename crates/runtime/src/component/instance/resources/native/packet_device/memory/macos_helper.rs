//! Same Root and actual registrar; the body waits until its witness is strong.
use super::*;
impl Memory {
    pub(in super::super) fn reserve_macos_job(
        &self,
        original: &original::Original,
        identity: &zero_platform_tokio::contract::component::PacketDeviceIdentity,
        ordinal: usize,
    ) -> Result<(), ChannelError> {
        let tasks = self.tasks.as_ref().ok_or_else(|| {
            error(
                identity.instance().component,
                Reason::Internal,
                "same helper has no original registrar",
            )
        })?;
        self.roots
            .process
            .reserve_macos_tun_helper(
                identity,
                ordinal,
                original.helper_job_original(),
                tasks,
                &self.roots.packet.driver_metadata_view(),
            )
            .map_err(|source| {
                error(
                    identity.instance().component,
                    if source.kind() == std::io::ErrorKind::WouldBlock {
                        Reason::Capacity
                    } else {
                        metadata_reason(&source)
                    },
                    "actual helper graph cannot fit before native IO",
                )
                .with_source(source)
            })
    }

    pub(in super::super) fn prepare_macos_helper(
        &self,
        original: &Arc<original::Original>,
        binding: &PacketDeviceBinding,
        component: zero_core::component::ComponentId,
        output_limit: usize,
        request_capacity: usize,
    ) -> Result<zero_tun::MacosHelperAccess, ChannelError> {
        let dedicated = matches!(binding, PacketDeviceBinding::Create { .. })
            && zero_tun::macos_utun_create_needs_helper_worker();
        // This actual launch is held before native FD/authorization syscalls.
        // Descriptor/UID0 have no invented helper worker or phantom TaskId.
        let launch = if dedicated {
            Some(
                self.tasks
                    .as_ref()
                    .ok_or_else(|| {
                        error(
                            component,
                            Reason::Internal,
                            "Kernel helper lacks actual registrar",
                        )
                    })?
                    .reserve_launch(zero_platform_tokio::contract::scope::TaskClass::Critical)
                    .map_err(|source| {
                        error(
                            component,
                            launch_reason(source),
                            "actual helper launch cannot fit before native acquisition",
                        )
                        .with_source(source)
                    })?,
            )
        } else {
            None
        };
        let read = self.prepare(original, component)?;
        let memory = read.memory_view().map_err(|source| {
            error(
                component,
                Reason::Revoked,
                "actual helper memory grant revoked",
            )
            .with_source(source)
        })?;
        let (access, work) = original
            .prepare_macos_helper(
                &memory,
                output_limit,
                request_capacity,
                dedicated,
                component,
            )
            .map_err(|source| {
                error(
                    component,
                    metadata_reason(&source),
                    "actual helper metadata cannot fit before acquisition",
                )
                .with_source(source)
            })?;
        if let (Some(launch), Some(work)) = (launch, work) {
            let registered = launch.spawn_blocking_tracked(move || work.run());
            match registered {
                Ok((_, witness)) => {
                    *self
                        .reader
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(witness);
                }
                Err(source) => {
                    // The actual registrar rejected without a TaskId/body.
                    original.stop_macos();
                    original.observe_helper_driver_returned();
                    return Err(error(
                        component,
                        launch_reason(source),
                        "actual helper registration rejected before any native attempt",
                    )
                    .with_source(source));
                }
            }
        }
        Ok(access)
    }
    pub(in super::super) fn helper_witness(&self) -> Option<TaskJoinWitness> {
        self.reader
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
    pub(in super::super) fn accept_helper_failure(
        &self,
        original: &original::Original,
        witness: &TaskJoinWitness,
        source: Arc<ChannelError>,
    ) -> bool {
        let Some(actual) = self.helper_witness() else {
            return false;
        };
        if !actual.same_registration(witness) {
            return false;
        }
        let Some(joined) = witness.try_joined() else {
            return false;
        };
        let Err(actual_source) = joined.result() else {
            return false;
        };
        if !Arc::ptr_eq(&actual_source, &source) {
            return false;
        }
        if !original.receive_helper_actor_failure(source) {
            return false;
        }
        // Exact source is strong in its finite original before external wake.
        witness.acknowledge_hosted_failure(&joined).is_ok()
    }
    pub(in super::super) fn helper_returned(
        &self,
        original: &original::Original,
        witness: &TaskJoinWitness,
    ) -> bool {
        let Some(actual) = self.helper_witness() else {
            return false;
        };
        if !actual.same_registration(witness) {
            return false;
        }
        let Some(joined) = witness.try_joined() else {
            return false;
        };
        if !joined.registration_released() {
            return false;
        }
        if let Err(source) = joined.result() {
            if !original.helper_actor_failure_received(&source) {
                return false;
            }
        }
        original.observe_helper_driver_returned();
        true
    }
}

fn launch_reason(source: zero_platform_tokio::contract::scope::SpawnError) -> Reason {
    use zero_platform_tokio::contract::scope::SpawnError;
    match source {
        SpawnError::Capacity | SpawnError::IdentityExhausted => Reason::Capacity,
        SpawnError::Closing => Reason::Revoked,
        SpawnError::NotStarted => Reason::Internal,
    }
}
fn metadata_reason(source: &std::io::Error) -> Reason {
    use zero_tun::packet_memory::PacketMemoryError;
    match source
        .get_ref()
        .and_then(|source| source.downcast_ref::<PacketMemoryError>())
    {
        Some(PacketMemoryError::Stopped) => Reason::Revoked,
        Some(_) => Reason::Capacity,
        None => Reason::Io,
    }
}

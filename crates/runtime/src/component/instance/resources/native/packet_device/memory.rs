//! Actual read workspaces and the SDK's original internal reader registration.
use super::*;
use zero_platform_tokio::contract::scope::{HostedCallbacks, WeakTaskRegistrar};
use zero_tun::packet_memory::NativeReadGrant;
#[cfg(target_os = "macos")]
mod macos_helper;
#[cfg(windows)]
mod ring;
pub(super) struct Memory {
    roots: Arc<crate::HostOriginalBudgets>,
    tasks: Option<WeakTaskRegistrar>,
    reads: Mutex<Reads>,
    #[cfg(any(target_os = "windows", target_os = "macos"))]
    reader: Mutex<Option<TaskJoinWitness>>,
    #[cfg(windows)]
    ring: Mutex<Option<zero_tun::packet_memory::RequestedRingCapacityCredit>>,
    #[cfg(windows)]
    session_metadata: Mutex<Option<zero_tun::packet_memory::PacketMetadataCredit>>,
}
#[derive(Default)]
struct Reads {
    stopped: bool,
    values: Option<[Option<NativeReadGrant>; 3]>,
}
impl Memory {
    pub(super) fn new(
        roots: Arc<crate::HostOriginalBudgets>,
        tasks: Option<WeakTaskRegistrar>,
    ) -> Self {
        Self {
            roots,
            tasks,
            reads: Mutex::new(Reads::default()),
            #[cfg(any(target_os = "windows", target_os = "macos"))]
            reader: Mutex::new(None),
            #[cfg(windows)]
            ring: Mutex::new(None),
            #[cfg(windows)]
            session_metadata: Mutex::new(None),
        }
    }
    pub(super) fn callbacks(&self) -> Option<HostedCallbacks> {
        self.tasks
            .as_ref()
            .and_then(WeakTaskRegistrar::defer_callbacks)
    }
    pub(super) fn prepare(
        &self,
        original: &original::Original,
        component: zero_core::component::ComponentId,
    ) -> Result<NativeReadGrant, ChannelError> {
        {
            let held = self
                .reads
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if held.stopped {
                return Err(error(
                    component,
                    Reason::Revoked,
                    "same original memory admission stopped",
                ));
            }
            #[cfg(target_os = "macos")]
            if let Some(read) = held
                .values
                .as_ref()
                .and_then(|reads| reads[0].as_ref())
                .cloned()
            {
                return Ok(read);
            }
        }
        #[cfg(windows)]
        self.prepare_ring(component)?;
        #[cfg(target_os = "macos")]
        let (reads, macos_write) = {
            // Utun's four-byte AF prefix needs its original fixed backend
            // read/write workspaces in addition to the Source's raw-IP read.
            // One lock prepays both actual lanes before any FD/helper attempt.
            let work = self
                .roots
                .packet
                .datagram_workspaces([65_536, 65_540], Some(65_540))
                .map_err(|source| {
                    error(
                        component,
                        Reason::Capacity,
                        "utun original workspaces cannot fit before acquisition",
                    )
                    .with_source(source)
                })?;
            let write = work.control.expect("same prepaid utun write workspace");
            let [first, second] = work.reads;
            ([Some(first), Some(second), Some(write.clone())], write)
        };
        #[cfg(not(target_os = "macos"))]
        let reads = {
            let failure = |source| {
                error(
                    component,
                    Reason::Capacity,
                    "native read workspaces cannot fit before acquisition",
                )
                .with_source(source)
            };
            if cfg!(target_os = "windows") && self.tasks.is_some() {
                let work = self
                    .roots
                    .packet
                    .datagram_workspaces([65_536; 2], None)
                    .map_err(failure)?;
                let [first, second] = work.reads;
                [Some(first), Some(second), None]
            } else {
                [
                    Some(self.roots.packet.native_grant(65_536).map_err(failure)?),
                    None,
                    None,
                ]
            }
        };
        let actual = reads[0].as_ref().expect("same actual primary read").clone();
        #[cfg(target_os = "windows")]
        let sdk = reads[1].clone();
        #[cfg(target_os = "macos")]
        let macos_read = reads[1]
            .as_ref()
            .expect("same actual utun prefix read")
            .clone();
        let mut held = self
            .reads
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if held.stopped || held.values.is_some() {
            return Err(error(
                component,
                Reason::Revoked,
                "same original native workspace already issued",
            ));
        }
        held.values = Some(reads);
        drop(held);
        #[cfg(target_os = "macos")]
        original
            .prepare_macos_workspaces(macos_read, macos_write)
            .map_err(|source| {
                error(
                    component,
                    Reason::Io,
                    "same utun workspace installation failed",
                )
                .with_source(source)
            })?;
        #[cfg(target_os = "windows")]
        if let Some(grant) = sdk {
            // The actual SDK reader needs its local AND Host-global launch
            // before prepare_reader creates the original native event. This
            // reserves no task/witness and cannot bypass the Host start fence.
            let launch = self
                .tasks
                .as_ref()
                .expect("hosted SDK reader has actual registrar")
                .reserve_launch(zero_platform_tokio::contract::scope::TaskClass::Critical)
                .map_err(|source| {
                    error(
                        component,
                        Reason::Capacity,
                        "same Scope rejected SDK reader before native preparation",
                    )
                    .with_source(source)
                })?;
            let capacity = self
                .ring
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
                .expect("same preinstalled SDK request")
                .capacity();
            let work = original.prepare_reader(grant, capacity).map_err(|source| {
                error(component, Reason::Io, "same SDK reader preparation failed")
                    .with_source(source)
            })?;
            let (_, witness) = launch
                .spawn_blocking_tracked(move || {
                    work.run().map_err(|source| {
                        error(component, Reason::Io, "actual Wintun reader failed")
                            .with_source(source)
                    })
                })
                .map_err(|source| {
                    error(
                        component,
                        Reason::Capacity,
                        "same Scope rejected the original SDK reader",
                    )
                    .with_source(source)
                })?;
            *self
                .reader
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(witness);
        }
        #[cfg(not(target_os = "windows"))]
        let _ = original;
        Ok(actual)
    }
    pub(super) fn stop(&self, original: &original::Original) {
        let reads = {
            let mut state = self
                .reads
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.stopped = true;
            state.values.take()
        };
        if let Some(reads) = reads {
            for read in reads.into_iter().flatten() {
                read.revoke();
            }
        }
        original.stop_reader();
    }
    pub(super) fn joined(&self) -> bool {
        #[cfg(any(target_os = "windows", target_os = "macos"))]
        {
            self.reader
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
                .is_none_or(TaskJoinWitness::is_joined)
        }
        #[cfg(not(any(target_os = "windows", target_os = "macos")))]
        true
    }
    pub(super) async fn join(&self, deadline: Instant) -> bool {
        #[cfg(any(target_os = "windows", target_os = "macos"))]
        {
            let reader = self
                .reader
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            if let Some(reader) = reader {
                return tokio::time::timeout_at(deadline, reader.wait())
                    .await
                    .is_ok();
            }
        }
        #[cfg(not(any(target_os = "windows", target_os = "macos")))]
        let _ = deadline;
        true
    }
}
impl PacketDeviceOwner {
    pub(super) fn memory_callbacks(&self) -> Option<HostedCallbacks> {
        self.memory.callbacks()
    }
}

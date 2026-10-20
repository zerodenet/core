//! Canonical ancillary originals. No task registry, protocol defaults or plain
//! UDP fallback: one installed owner per actual declaration ordinal.
use super::*;
use zero_core::component::ComponentInstanceId;
use zero_platform_tokio::{
    contract::{
        component::{GrantedMetadataListener, MetadataDatagramRequest},
        scope::TaskScopeIdentity,
    },
    controlled_datagram::{
        metadata::{MetadataDatagramControl, MetadataLimits, MetadataOwner},
        DatagramOwnerReport,
    },
};
mod acquisition;
mod evidence;
pub(in crate::component) use evidence::MetadataIoEvidence;

pub(super) struct MetadataDeclarations {
    identity: Option<(ComponentInstanceId, TaskScopeIdentity)>,
    entries: BTreeMap<usize, Entry>,
}
struct Entry {
    source: SocketAddr,
    limits: MetadataLimits,
    state: Mutex<Native>,
}
struct Native {
    owner: MetadataOwner,
    // Same originals, installed before observation transfer or any postcheck.
    controls: Vec<MetadataDatagramControl>,
    remaining: usize,
    attempted: bool,
    closing: bool,
    main_busy: bool,
    probes_busy: bool,
    main: evidence::Originals,
    probes: evidence::Originals,
    first_fault: Option<Arc<ChannelError>>,
    pending_main_closed: u64,
    pending_probe_closed: u64,
}
impl MetadataDeclarations {
    pub(super) fn new(
        component: ComponentId,
        declarations: &[ResourceDeclaration],
        identity: Option<(ComponentInstanceId, TaskScopeIdentity)>,
    ) -> Result<Self, ChannelError> {
        let mut entries = BTreeMap::new();
        for (ordinal, declaration) in declarations.iter().enumerate() {
            let ResourceDeclaration::MetadataDatagram {
                source,
                limits,
                max_bind_attempts,
            } = declaration
            else {
                continue;
            };
            if identity.is_none() {
                return Err(error(
                    component,
                    Reason::Unsupported,
                    "metadata acquisition requires the actual instance and host scope",
                ));
            }
            let owner = MetadataOwner::new(*limits).map_err(|source| {
                error(component, Reason::InvalidConfig, "invalid metadata limits")
                    .with_source(source)
            })?;
            entries.insert(
                ordinal,
                Entry {
                    source: *source,
                    limits: *limits,
                    state: Mutex::new(Native {
                        owner,
                        controls: Vec::new(),
                        remaining: max_bind_attempts.get(),
                        attempted: false,
                        closing: false,
                        main_busy: false,
                        probes_busy: false,
                        main: Default::default(),
                        probes: Default::default(),
                        first_fault: None,
                        pending_main_closed: 0,
                        pending_probe_closed: 0,
                    }),
                },
            );
        }
        Ok(Self { identity, entries })
    }
    /// Call outside every HostState/cleanup lock. Reentrant native wakes can
    /// inspect this provider; a busy original stays pending instead of blocking.
    pub(super) fn close_native(&self, component: ComponentId) {
        for entry in self.entries.values() {
            let Some(mut native) = entry.lock() else {
                continue;
            };
            native.closing = true;
            native.close_originals(component);
            // Only the same owner's explicit report permits dropping controls.
            if !native.main_busy
                && !native.probes_busy
                && native.main.complete()
                && native.probes.complete()
            {
                native.controls.clear();
            }
        }
    }
    pub(super) fn append_receipts(
        &self,
        component: ComponentId,
        report: &mut zero_platform_tokio::contract::lifecycle::CloseReport,
    ) {
        for (ordinal, entry) in &self.entries {
            let Some(mut native) = entry.lock() else {
                continue;
            };
            if native.pending_main_closed != 0 {
                report.closed_io.push(format!("component {} metadata declaration {ordinal} main: {} original native closes confirmed",component.0,native.pending_main_closed));
                native.pending_main_closed = 0;
            }
            if native.pending_probe_closed != 0 {
                report.closed_io.push(format!("component {} metadata declaration {ordinal} probes: {} original native closes confirmed",component.0,native.pending_probe_closed));
                native.pending_probe_closed = 0;
            }
        }
    }
}
impl Entry {
    fn lock(&self) -> Option<std::sync::MutexGuard<'_, Native>> {
        match self.state.try_lock() {
            Ok(guard) => Some(guard),
            Err(std::sync::TryLockError::Poisoned(error)) => Some(error.into_inner()),
            Err(std::sync::TryLockError::WouldBlock) => None,
        }
    }
}
impl Native {
    fn close_originals(&mut self, component: ComponentId) {
        self.main_busy = true;
        if let Some(main) = self.owner.try_close_main() {
            self.record_main(component, main);
        }
        self.probes_busy = true;
        if let Some(probes) = self.owner.try_close_probes() {
            self.record_probes(component, probes);
        }
    }
    fn observe_originals(&mut self, component: ComponentId) {
        self.main_busy = true;
        if let Some(main) = self.owner.try_take_main_report() {
            self.record_main(component, main);
        }
        self.probes_busy = true;
        if let Some(probes) = self.owner.try_take_probe_report() {
            self.record_probes(component, probes);
        }
    }
    fn record_main(&mut self, component: ComponentId, report: DatagramOwnerReport) {
        self.main_busy = false;
        self.pending_main_closed += report.confirmed_closed;
        self.main.record(report);
        self.record_fault(component);
    }
    fn record_probes(&mut self, component: ComponentId, report: DatagramOwnerReport) {
        self.probes_busy = false;
        self.pending_probe_closed += report.confirmed_closed;
        self.probes.record(report);
        self.record_fault(component);
    }
    fn record_fault(&mut self, component: ComponentId) {
        if self.first_fault.is_none() {
            if let Some(source) = self
                .main
                .unknown
                .first()
                .or_else(|| self.probes.unknown.first())
            {
                let mut failure = zero_platform_tokio::contract::ChannelError::new(
                    zero_platform_tokio::contract::Stage::Close,
                    Reason::Io,
                    zero_platform_tokio::contract::Impact::Component,
                );
                failure.component = Some(component);
                failure.delivery = zero_platform_tokio::contract::DeliveryState::Unknown;
                self.first_fault = Some(Arc::new(
                    failure.with_source(evidence::NativeSource(source.source.clone())),
                ));
            }
        }
    }
}

#[cfg(test)]
#[path = "../../../tests/resource_metadata.rs"]
mod tests;

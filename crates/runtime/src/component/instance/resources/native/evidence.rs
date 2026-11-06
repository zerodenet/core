pub(in crate::component) use super::files::evidence::FileIoEvidence;
use super::*;

/// Nonconsuming facts from the actual installed owners. Sticky faults remain
/// independent of physical completion and never turn Unknown into an ACK.
#[derive(Clone, Debug)]
pub(in crate::component) struct HostIoEvidence {
    pub declared_count: usize,
    pub icmp: Option<bool>,
    pub declarations: Vec<super::coverage::DeclarationEvidence>,
    pub admission_closed: bool,
    pub files: Vec<FileIoEvidence>,
    pub native: Vec<NativeIoEvidence>,
    pub roots: Vec<super::root_datagram::RootDatagramEvidence>,
    pub packets: Vec<super::packet_device::PacketDeviceEvidence>,
    pub bounds: Vec<crate::component::bound_datagram::evidence::BoundDatagramEvidence>,
    pub carriers: Vec<super::carrier::CarrierIoEvidence>,
    pub metadata: Vec<super::metadata::MetadataIoEvidence>,
    pub metapools: Vec<super::metapool::MetadataPoolEvidence>,
    #[cfg(target_os = "linux")]
    pub processes: Vec<super::process::ProcessIoEvidence>,
    pub sticky_faults: Vec<Arc<ChannelError>>,
}
#[derive(Clone, Debug)]
pub(in crate::component) struct NativeIoEvidence {
    pub tcp: bool,
    pub address: SocketAddr,
    pub confirmed: bool,
    pub known_no_acquisition: bool,
    #[cfg(test)]
    pub first_fault: Option<Arc<ChannelError>>,
}
impl HostResources {
    pub(in crate::component) fn io_evidence(&self) -> HostIoEvidence {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut sticky_faults: Vec<_> = state.io.failures.values().cloned().collect();
        sticky_faults.extend(state.files.failure.iter().cloned());
        let native: Vec<_> = state
            .tcp
            .keys()
            .map(|address| (true, *address))
            .chain(state.datagram.keys().map(|address| (false, *address)))
            .map(|key| NativeIoEvidence {
                tcp: key.0,
                address: key.1,
                confirmed: state.io.confirmed.contains(&key),
                // Never block behind cleanup/acquisition. Busy stays pending.
                known_no_acquisition: if key.0 {
                    state.tcp[&key.1]
                        .try_lock()
                        .is_ok_and(|owner| !owner.attempted)
                } else {
                    state.datagram[&key.1]
                        .try_lock()
                        .is_ok_and(|owner| owner.known_no_acquisition())
                },
                #[cfg(test)]
                first_fault: state.io.failures.get(&key).cloned(),
            })
            .collect();
        let files = state.files.evidence();
        let admission_closed = state.closed || self.cancel.is_cancelled();
        drop(state);
        let roots = self.roots.evidence(self.component);
        sticky_faults.extend(roots.iter().filter_map(|entry| entry.first_fault.clone()));
        let carriers = self.carriers.evidence();
        let metadata = self.metadata.evidence(self.component);
        let metapools = self.metapools.evidence();
        sticky_faults.extend(
            metapools
                .iter()
                .filter_map(|entry| entry.first_fault.clone()),
        );
        sticky_faults.extend(
            metadata
                .iter()
                .filter_map(|entry| entry.first_fault.clone()),
        );
        sticky_faults.extend(
            carriers
                .iter()
                .filter_map(|carrier| carrier.first_fault.clone()),
        );
        let bounds = self.bounds.evidence();
        sticky_faults.extend(bounds.iter().filter_map(|owner| owner.first_fault.clone()));
        let icmp = self.icmp.as_ref().map(|owner| owner.physical_confirmed());
        sticky_faults.extend(self.icmp.as_ref().and_then(|owner| owner.fault()));
        let packets: Vec<_> = self.packets.iter().map(|owner| owner.evidence()).collect();
        #[cfg(target_os = "linux")]
        let processes = self.processes.evidence(self.component);
        #[cfg(target_os = "linux")]
        sticky_faults.extend(
            processes
                .iter()
                .filter_map(|owner| owner.first_fault.clone()),
        );
        sticky_faults.extend(packets.iter().filter_map(|owner| owner.close_fault.clone()));
        let declarations = self
            .coverage
            .iter()
            .enumerate()
            .map(|(ordinal, binding)| {
                let mut proof = binding.observe(ordinal, &files, &native);
                if matches!(binding, coverage::CoverageBinding::Icmp) && icmp.is_some() {
                    proof.provider = coverage::DeclarationProvider::Icmp;
                }
                if matches!(binding, coverage::CoverageBinding::Carrier(_)) {
                    proof.provider = carriers
                        .iter()
                        .position(|carrier| {
                            carrier.ordinal == ordinal
                                && carrier.kind == proof.kind
                                && carrier.registered
                        })
                        .map(coverage::DeclarationProvider::Carrier)
                        .unwrap_or(coverage::DeclarationProvider::Unaccounted);
                }
                if matches!(binding, coverage::CoverageBinding::Metadata) {
                    proof.provider = metadata
                        .iter()
                        .position(|entry| entry.ordinal == ordinal)
                        .map(coverage::DeclarationProvider::Metadata)
                        .unwrap_or(coverage::DeclarationProvider::Unaccounted);
                }
                if matches!(binding, coverage::CoverageBinding::MetadataPool) {
                    proof.provider = metapools
                        .iter()
                        .position(|entry| entry.ordinal == ordinal)
                        .map(coverage::DeclarationProvider::MetadataPool)
                        .unwrap_or(coverage::DeclarationProvider::Unaccounted);
                }
                if matches!(binding, coverage::CoverageBinding::RootDatagram) {
                    proof.provider = roots
                        .iter()
                        .position(|root| root.ordinal == ordinal)
                        .map(coverage::DeclarationProvider::RootDatagram)
                        .unwrap_or(coverage::DeclarationProvider::Unaccounted);
                }
                if matches!(binding, coverage::CoverageBinding::PacketDevice) {
                    proof.provider = packets
                        .iter()
                        .position(|owner| owner.ordinal == ordinal)
                        .map(coverage::DeclarationProvider::PacketDevice)
                        .unwrap_or(coverage::DeclarationProvider::Unaccounted);
                }
                if matches!(binding, coverage::CoverageBinding::BoundDatagram) {
                    proof.provider = bounds
                        .iter()
                        .position(|owner| owner.ordinal == ordinal)
                        .map(coverage::DeclarationProvider::BoundDatagram)
                        .unwrap_or(coverage::DeclarationProvider::Unaccounted);
                }
                #[cfg(target_os = "linux")]
                if matches!(
                    binding,
                    coverage::CoverageBinding::Unaccounted(coverage::DeclaredResourceKind::Process)
                ) {
                    proof.provider = processes
                        .iter()
                        .position(|owner| owner.ordinal == ordinal && owner.registered)
                        .map(coverage::DeclarationProvider::Process)
                        .unwrap_or(coverage::DeclarationProvider::Unaccounted);
                }
                proof
            })
            .collect();
        HostIoEvidence {
            declared_count: self.coverage.len(),
            icmp,
            declarations,
            admission_closed,
            files,
            native,
            carriers,
            roots,
            packets,
            bounds,
            metadata,
            metapools,
            #[cfg(target_os = "linux")]
            processes,
            sticky_faults,
        }
    }
}

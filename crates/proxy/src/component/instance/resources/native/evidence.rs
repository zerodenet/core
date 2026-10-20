pub(in crate::component) use super::files::evidence::FileIoEvidence;
use super::*;

/// Nonconsuming facts from the actual installed owners. Sticky faults remain
/// independent of physical completion and never turn Unknown into an ACK.
#[derive(Clone, Debug)]
pub(in crate::component) struct HostIoEvidence {
    pub declared_count: usize,
    pub declarations: Vec<super::coverage::DeclarationEvidence>,
    pub admission_closed: bool,
    pub files: Vec<FileIoEvidence>,
    pub native: Vec<NativeIoEvidence>,
    pub sticky_faults: Vec<Arc<ChannelError>>,
}
#[derive(Clone, Debug)]
pub(in crate::component) struct NativeIoEvidence {
    pub tcp: bool,
    pub address: SocketAddr,
    pub confirmed: bool,
    pub known_no_acquisition: bool,
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
                first_fault: state.io.failures.get(&key).cloned(),
            })
            .collect();
        let files = state.files.evidence();
        let declarations = self
            .coverage
            .iter()
            .enumerate()
            .map(|(ordinal, binding)| binding.observe(ordinal, &files, &native))
            .collect();
        HostIoEvidence {
            declared_count: self.coverage.len(),
            declarations,
            admission_closed: state.closed || self.cancel.is_cancelled(),
            files,
            native,
            sticky_faults,
        }
    }
}

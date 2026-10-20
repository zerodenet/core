pub(in crate::component) use super::files::evidence::FileIoEvidence;
use super::*;

/// Nonconsuming facts from the actual installed owners. Sticky faults remain
/// independent of physical completion and never turn Unknown into an ACK.
#[derive(Clone, Debug)]
pub(in crate::component) struct HostIoEvidence {
    pub files: Vec<FileIoEvidence>,
    pub native: Vec<NativeIoEvidence>,
    pub sticky_faults: Vec<Arc<ChannelError>>,
}
#[derive(Clone, Debug)]
pub(in crate::component) struct NativeIoEvidence {
    pub tcp: bool,
    pub address: SocketAddr,
    pub confirmed: bool,
    pub first_fault: Option<Arc<ChannelError>>,
}
impl HostIoEvidence {
    pub(in crate::component) fn physical_confirmed(&self) -> bool {
        self.files.iter().all(FileIoEvidence::physical_confirmed)
            && self.native.iter().all(|native| native.confirmed)
    }
}
impl HostResources {
    pub(in crate::component) fn io_evidence(&self) -> HostIoEvidence {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut sticky_faults: Vec<_> = state.io.failures.values().cloned().collect();
        sticky_faults.extend(state.files.failure.iter().cloned());
        let native = state
            .tcp
            .keys()
            .map(|address| (true, *address))
            .chain(state.datagram.keys().map(|address| (false, *address)))
            .map(|key| NativeIoEvidence {
                tcp: key.0,
                address: key.1,
                confirmed: state.io.confirmed.contains(&key),
                first_fault: state.io.failures.get(&key).cloned(),
            })
            .collect();
        HostIoEvidence {
            files: state.files.evidence(),
            native,
            sticky_faults,
        }
    }
}

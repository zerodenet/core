//! Capability probes are original sockets in an owner that predates creation.
use super::*;
use quinn_udp::controlled::{NativeProbes, ProbeKind};
#[derive(Debug)]
pub(super) struct Probes {
    pub(super) owner: ControlledDatagramOwner,
    limit: NonZeroUsize,
    attempts: usize,
    /// Bounded by this initialization's native attempt and capability counts.
    pub(super) diagnostics: Vec<Arc<std_io::Error>>,
}
impl Probes {
    pub(super) fn new(sockets: NonZeroUsize, attempts: NonZeroUsize) -> Self {
        Self {
            owner: ControlledDatagramOwner::new(sockets),
            limit: attempts,
            attempts: 0,
            diagnostics: Vec::new(),
        }
    }
    pub(super) fn begin(&mut self) {
        self.attempts = 0;
        self.diagnostics.clear();
    }
    pub(super) fn check_unknown(&self) -> Result<(), Arc<std_io::Error>> {
        for entry in self.owner.entries.values() {
            if let NativeHandleState::Unknown(source) = &*entry.lock() {
                return Err(source.clone());
            }
        }
        Ok(())
    }
    fn create(&mut self) -> std_io::Result<ControlledDatagramSocket> {
        let mut last = None;
        for address in [
            SocketAddr::from(([0u16; 8], 0)),
            SocketAddr::from(([127, 0, 0, 1], 0)),
        ] {
            self.attempts = self
                .attempts
                .checked_add(1)
                .filter(|n| *n <= self.limit.get())
                .ok_or_else(|| {
                    std_io::Error::new(
                        std_io::ErrorKind::WouldBlock,
                        "UDP native probe attempt capacity exhausted",
                    )
                })?;
            match self.owner.bind_raw_listener(address) {
                Ok(socket) => return Ok(socket),
                Err(error) => {
                    self.diagnostics.push(error.source.clone());
                    // A refusal before reservation is a provider/budget fault,
                    // not evidence that this native family lacks capability.
                    if error.reservation.is_none()
                        || matches!(error.outcome, AcquisitionOutcome::Unknown(_))
                    {
                        return Err(std_io::Error::other(error));
                    }
                    // KnownNoNative or a real close receipt precedes fallback.
                    last = Some(error);
                }
            }
        }
        Err(std_io::Error::other(last.expect("two native attempts")))
    }
}
impl NativeProbes for Probes {
    fn detect(&mut self, kind: ProbeKind) -> std_io::Result<usize> {
        if let Some(value) = quinn_udp::controlled::capability_without_probe(kind) {
            return Ok(value);
        }
        let socket = self.create()?;
        let result = {
            let native = socket.entry.lock();
            let NativeHandleState::Raw(original) = &*native else {
                return Err(std_io::Error::other("owned native probe is not raw"));
            };
            quinn_udp::controlled::probe_owned_socket(kind, original.into())
        };
        let result = result.map_err(|error| {
            let error = Arc::new(error);
            *socket
                .entry
                .acquisition_failure
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(error.clone());
            error
        });
        // Even option failure is not permission to abandon an actual probe.
        // Confirm the original before publishing capability or using fallback.
        socket.close().map_err(std_io::Error::other)?;
        self.owner.reap_confirmed(); // Commit receipt before the next operation.
        match result {
            Ok(value) => Ok(value),
            Err(error) => {
                self.diagnostics.push(error);
                // The original algorithm reports 1 on this option failure.
                // Capacity/acquisition/unknown-close never reaches this branch.
                Ok(1)
            }
        }
    }
}

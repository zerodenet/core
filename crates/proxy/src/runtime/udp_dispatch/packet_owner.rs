//! A finite original-socket slot retained by the actual candidate peer.
//! No request budget, service source, runtime, or returned I/O view owns it.
mod demand;
pub(crate) use demand::PacketCarrierDemand;
#[cfg(test)]
mod tests;
use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};
use zero_engine::EngineError;
use zero_platform_tokio::contract::network::UpstreamBinding;
use zero_platform_tokio::contract::CancellationToken;
use zero_platform_tokio::controlled_datagram::{ControlledDatagramOwner, DatagramOwnerReport};

pub(crate) struct PacketCarrierOwnerSlot {
    state: Mutex<State>,
    stopped: CancellationToken,
}
struct State {
    // One physical root per candidate peer. Unknown/retiring originals count.
    native: ControlledDatagramOwner,
    binding: Option<UpstreamBinding>,
    attempted: bool,
    building: bool,
    failure: Option<Arc<EngineError>>,
}
pub(crate) struct PacketCarrierCloseReport {
    pub(crate) native: DatagramOwnerReport,
    pub(crate) building: bool,
    pub(crate) binding: Option<UpstreamBinding>,
    pub(crate) failure: Option<Arc<EngineError>>,
}
impl PacketCarrierCloseReport {
    pub(crate) fn is_complete(&self) -> bool {
        !self.building && self.native.is_complete()
    }
}
pub(crate) struct BuildAttempt {
    owner: Arc<PacketCarrierOwnerSlot>,
}
impl PacketCarrierOwnerSlot {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                native: ControlledDatagramOwner::new(NonZeroUsize::MIN),
                binding: None,
                attempted: false,
                building: false,
                failure: None,
            }),
            stopped: CancellationToken::new(),
        })
    }
    pub(crate) fn begin(
        self: &Arc<Self>,
        binding: &UpstreamBinding,
    ) -> Result<BuildAttempt, EngineError> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if self.stopped.is_cancelled() || state.attempted {
            return Err(super::packet_path_operation::unsupported(
                "candidate packet carrier slot is retired or already attempted",
            ));
        }
        state.binding = Some(binding.clone());
        state.attempted = true;
        state.building = true;
        Ok(BuildAttempt {
            owner: self.clone(),
        })
    }
    pub(crate) fn revoke(&self) {
        self.stopped.cancel();
    }
    /// Actual native closure only; a pending build and any other task graph
    /// remain separate obligations. Retrying uses this same original owner.
    pub(crate) fn close_native(&self) -> PacketCarrierCloseReport {
        self.revoke();
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        PacketCarrierCloseReport {
            native: state.native.close_all(),
            building: state.building,
            binding: state.binding.clone(),
            failure: state.failure.clone(),
        }
    }
}
impl BuildAttempt {
    pub(crate) fn stopped(&self) -> &CancellationToken {
        &self.owner.stopped
    }
    pub(crate) fn with_native<T>(
        &self,
        acquire: impl FnOnce(&mut ControlledDatagramOwner) -> Result<T, EngineError>,
    ) -> Result<T, EngineError> {
        let mut state = self
            .owner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if self.owner.stopped.is_cancelled() {
            return Err(super::packet_path_operation::unsupported(
                "candidate packet carrier was stopped",
            ));
        }
        acquire(&mut state.native)
    }
    pub(crate) fn retain_failure(&self, error: EngineError) -> EngineError {
        let mut state = self
            .owner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let source = state.failure.get_or_insert_with(|| Arc::new(error)).clone();
        EngineError::Io(std::io::Error::other(SharedBuildFailure(source)))
    }
}
impl Drop for BuildAttempt {
    fn drop(&mut self) {
        self.owner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .building = false;
    }
}
#[derive(Debug)]
struct SharedBuildFailure(Arc<EngineError>);
impl std::fmt::Display for SharedBuildFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for SharedBuildFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

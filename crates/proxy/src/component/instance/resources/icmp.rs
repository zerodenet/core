//! Same-instance host authority for one actual controlled ICMP pool. Not a
//! ResourceAccess implementation, UDP grant or second owner of a declaration.
mod grant;
mod lifecycle;
use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex, MutexGuard, Weak},
};
use tokio::time::Instant;
use zero_core::component::ComponentInstanceId;
use zero_platform_tokio::{
    contract::{
        component::ResourceDeclaration, lifecycle::CloseReport, CancellationToken, ChannelError,
        DeliveryState, Impact, Reason, Stage,
    },
    controlled_icmp::{ControlledIcmpOwner, IcmpOwnerReport},
};

pub(crate) struct HostIcmpProbeOwner {
    shared: Arc<Shared>,
}
#[derive(Clone)]
pub(crate) struct HostIcmpProbeGrant {
    shared: Weak<Shared>,
    instance: ComponentInstanceId,
}
struct Shared {
    instance: ComponentInstanceId,
    cancel: CancellationToken,
    state: Mutex<State>,
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Cold,
    Live,
    Revoked,
}
struct State {
    phase: Phase,
    native: ControlledIcmpOwner,
    status: IcmpOwnerReport,
    closed_total: u64,
    reported_total: u64,
    fault: Option<Arc<ChannelError>>,
}
impl Shared {
    fn try_state(&self) -> Result<MutexGuard<'_, State>, ChannelError> {
        match self.state.try_lock() {
            Ok(state) => Ok(state),
            Err(std::sync::TryLockError::Poisoned(error)) => Ok(error.into_inner()),
            Err(std::sync::TryLockError::WouldBlock) => Err(error(
                self.instance,
                Stage::Prepare,
                Reason::Capacity,
                "ICMP owner operation is already in progress",
            )),
        }
    }
    fn state(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}
impl HostIcmpProbeOwner {
    /// Pure construction. The registry installs this same owner before calling
    /// start or polling the rawIP driver; grant cannot activate itself.
    pub(crate) fn new(
        instance: ComponentInstanceId,
        declaration: &ResourceDeclaration,
        parent: CancellationToken,
    ) -> Result<Self, ChannelError> {
        let ResourceDeclaration::NativeIcmp { max_sockets } = declaration else {
            return Err(error(
                instance,
                Stage::Prepare,
                Reason::InvalidConfig,
                "not a native ICMP declaration",
            ));
        };
        Ok(Self::with_limit(instance, *max_sockets, parent))
    }
    fn with_limit(
        instance: ComponentInstanceId,
        limit: NonZeroUsize,
        parent: CancellationToken,
    ) -> Self {
        let cancel = parent.child_token();
        Self {
            shared: Arc::new(Shared {
                instance,
                cancel: cancel.clone(),
                state: Mutex::new(State {
                    phase: Phase::Cold,
                    native: ControlledIcmpOwner::new(limit, cancel),
                    status: IcmpOwnerReport::default(),
                    closed_total: 0,
                    reported_total: 0,
                    fault: None,
                }),
            }),
        }
    }
    pub(crate) fn grant(&self) -> HostIcmpProbeGrant {
        HostIcmpProbeGrant {
            shared: Arc::downgrade(&self.shared),
            instance: self.shared.instance,
        }
    }
    pub(crate) fn fault(&self) -> Option<Arc<ChannelError>> {
        self.shared
            .try_state()
            .ok()
            .and_then(|state| state.fault.clone())
    }
    /// Observation only; the caller separately proves real task/user closure.
    pub(crate) fn physical_confirmed(&self) -> bool {
        let mut state = match self.shared.state.try_lock() {
            Ok(state) => state,
            Err(std::sync::TryLockError::Poisoned(error)) => error.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => return false,
        };
        // Bind may unwind after acquiring an original. Never trust the last
        // cached snapshot: inspect this same persistent pool before proof.
        state.harvest();
        self.shared.cancel.is_cancelled() && state.status.is_complete()
    }
    /// Constant-space receipt aggregation. Err/cancellation does not lose any
    /// already confirmed originals; a report is extracted exactly once.
    pub(crate) fn take_closed_io(&self) -> CloseReport {
        match self.shared.state.try_lock() {
            Ok(mut state) => state.take_closed_io(self.shared.instance),
            Err(std::sync::TryLockError::Poisoned(error)) => {
                error.into_inner().take_closed_io(self.shared.instance)
            }
            Err(std::sync::TryLockError::WouldBlock) => CloseReport::default(),
        }
    }
}
impl State {
    fn harvest(&mut self) {
        self.status = self.native.take_report();
        self.closed_total += self.status.confirmed_closed;
    }
    fn take_closed_io(&mut self, instance: ComponentInstanceId) -> CloseReport {
        let count = self.closed_total - self.reported_total;
        self.reported_total = self.closed_total;
        let mut report = CloseReport::default();
        if count != 0 {
            report.closed_io.push(format!(
                "component {} generation {}: {count} ICMP originals explicitly closed",
                instance.component.0, instance.generation.0
            ));
        }
        report
    }
    fn remember(&mut self, failure: ChannelError) -> ChannelError {
        let source = Arc::new(failure);
        self.fault.get_or_insert_with(|| source.clone());
        crate::component::instance::retained(source)
    }
}
fn error(
    instance: ComponentInstanceId,
    stage: Stage,
    reason: Reason,
    message: &'static str,
) -> ChannelError {
    let mut error = ChannelError::new(stage, reason, Impact::Component)
        .with_source(std::io::Error::other(message));
    error.component = Some(instance.component);
    error.delivery = DeliveryState::NotSubmitted;
    error
}
#[derive(Debug)]
struct Source(Arc<std::io::Error>);
impl std::fmt::Display for Source {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for Source {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
fn native_error(
    instance: ComponentInstanceId,
    stage: Stage,
    source: Arc<std::io::Error>,
    unknown: bool,
) -> ChannelError {
    let mut error = error(instance, stage, Reason::Io, "native ICMP operation failed")
        .with_source(Source(source));
    if unknown {
        error.delivery = DeliveryState::Unknown;
    }
    error
}
#[cfg(test)]
#[path = "../../tests/icmp_resources.rs"]
mod tests;

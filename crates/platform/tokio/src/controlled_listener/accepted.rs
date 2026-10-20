//! Bounded children of the same listener owner. Native receipts never prove
//! transferred users or tasks completed; that host integration is Unaccounted.
mod accept;
mod close;
mod owner;
mod pool;
mod relay;
mod view;
use super::*;
use crate::contract::{lifecycle::CloseReport, ChannelError, DeliveryState, Impact, Reason, Stage};
use crate::{ControlledTcpControl, ControlledTcpStream};
use std::{collections::BTreeMap, num::NonZeroUsize};
use tokio::{sync::Mutex as AsyncMutex, time::Instant};
pub use view::AcceptedTcpStream;

pub struct AcceptedTcp {
    io: AcceptedTcpStream,
    claim: AcceptedTcpClaim,
}
impl AcceptedTcp {
    pub fn into_parts(self) -> (AcceptedTcpStream, AcceptedTcpClaim) {
        (self.io, self.claim)
    }
}
#[derive(Clone)]
pub struct AcceptedTcpClaim {
    entry: Arc<Entry>,
}
impl AcceptedTcpClaim {
    pub fn id(&self) -> u64 {
        self.entry.id
    }
    pub fn failure(&self) -> Option<Arc<ChannelError>> {
        self.entry.state().failure.clone()
    }
}
/// Only the original FD. No TaskJoined, user release or provider seal is asserted.
#[derive(Clone)]
pub struct AcceptedTcpNativeClosed {
    entry: Arc<Entry>,
}
impl AcceptedTcpNativeClosed {
    pub fn matches(&self, claim: &AcceptedTcpClaim) -> bool {
        Arc::ptr_eq(&self.entry, &claim.entry)
    }
    pub fn id(&self) -> u64 {
        self.entry.id
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcceptedTcpUsers {
    NoTransfer,
    Unaccounted,
}
pub struct AcceptedTcpEvidence {
    pub admission_closed: bool,
    pub retained: usize,
    pub unknown: Vec<(u64, Arc<ChannelError>)>,
    pub confirmed_native_closes: u64,
    pub users: AcceptedTcpUsers,
    pub first_failure: Option<Arc<ChannelError>>,
    pub first_close_failure: Option<Arc<ChannelError>>,
}
impl AcceptedTcpEvidence {
    pub fn native_confirmed(&self) -> bool {
        self.retained == 0
    }
    /// Accepted subresource only. The listener original and outer host scope
    /// still require their own real closure; this is never a whole slot seal.
    pub fn provider_confirmed(&self) -> bool {
        self.admission_closed
            && self.native_confirmed()
            && self.users == AcceptedTcpUsers::NoTransfer
    }
}
pub(super) struct Pool {
    identity: Arc<()>,
    limit: NonZeroUsize,
    cancel: CancellationToken,
    state: Mutex<PoolState>,
    relay: Arc<relay::AcceptRelay>,
}
struct PoolState {
    next: u64,
    entries: BTreeMap<u64, Arc<Entry>>,
    closed: u64,
    reported: u64,
    transferred: bool,
    first_failure: Option<Arc<ChannelError>>,
    first_close_failure: Option<Arc<ChannelError>>,
}
struct Entry {
    id: u64,
    identity: Arc<()>,
    state: Mutex<EntryState>,
    control: AsyncMutex<Option<ControlledTcpControl>>,
    uncertain: Arc<ChannelError>,
}
struct EntryState {
    phase: Phase,
    failure: Option<Arc<ChannelError>>,
    unknown: Option<Arc<ChannelError>>,
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Waiting,
    Acquiring,
    Installed,
    Closed,
    Unknown,
}
impl Entry {
    fn state(&self) -> std::sync::MutexGuard<'_, EntryState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}
fn failure(stage: Stage, reason: Reason, message: &'static str) -> ChannelError {
    let mut error =
        ChannelError::new(stage, reason, Impact::Component).with_source(io::Error::other(message));
    error.delivery = DeliveryState::NotSubmitted;
    error
}
#[derive(Debug)]
struct Retained(Arc<ChannelError>);
impl std::fmt::Display for Retained {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for Retained {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
fn retained(source: Arc<ChannelError>) -> ChannelError {
    let mut result = ChannelError::new(source.stage, source.reason, source.impact)
        .with_source(Retained(source.clone()));
    result.delivery = source.delivery;
    result.component = source.component;
    result.session = source.session;
    result.retry = source.retry;
    result
}

#[cfg(test)]
#[path = "../../tests/controlled_listener/accepted/mod.rs"]
mod tests;

//! Actual raw-IP child quotas retain every gateway lacking native closure proof.
use crate::runtime::raw_ip::tasks::Source;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, Weak},
};
use tokio::time::Instant;
use zero_platform_tokio::contract::{
    lifecycle::{CloseReport, ResourceLifecycle, Unfinished},
    ChannelError, CloseReason, Impact, Reason, Stage,
};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum ChildKind {
    Tcp,
    Udp,
    Dns,
}
impl ChildKind {
    fn index(self) -> usize {
        match self {
            Self::Tcp => 0,
            Self::Udp => 1,
            Self::Dns => 2,
        }
    }
}
#[derive(Clone)]
pub(crate) struct NativeObligations(Arc<Mutex<State>>);
struct State {
    closing: bool,
    next: u64,
    limits: [usize; 3],
    records: BTreeMap<u64, Record>,
    first_source: Option<Source>,
}
struct Record {
    kind: ChildKind,
    entered: bool,
    retiring: bool,
    caller: Option<Arc<crate::runtime::inbound_owners::HostPeerBinding>>,
    business_failure: Option<Arc<zero_engine::EngineError>>,
    carrier: Option<zero_platform_tokio::controlled_carrier::CarrierAdoption>,
    carrier_owner: Option<zero_platform_tokio::controlled_carrier::CarrierOwnerRegistration>,
    additional_carriers: Vec<zero_platform_tokio::controlled_carrier::CarrierAdoption>,
    carrier_conflict: bool,
}
pub(crate) struct NativePermit {
    owner: NativeObligations,
    id: u64,
}

impl NativeObligations {
    pub(crate) fn new(tcp: usize, udp: usize, dns: usize) -> Self {
        Self(Arc::new(Mutex::new(State {
            closing: false,
            next: 0,
            limits: [tcp, udp, dns],
            records: BTreeMap::new(),
            first_source: None,
        })))
    }
    pub(crate) fn resource(
        &self,
    ) -> Box<dyn zero_platform_tokio::contract::lifecycle::DynResourceLifecycle> {
        zero_platform_tokio::contract::lifecycle::erase_lifecycle(self.clone())
    }
    pub(crate) fn reserve(&self, kind: ChildKind) -> Result<NativePermit, ChannelError> {
        let mut state = self.0.lock().unwrap_or_else(|error| error.into_inner());
        if state.closing {
            return Err(rejected(Reason::Revoked));
        }
        if state
            .records
            .values()
            .filter(|record| record.kind == kind)
            .count()
            >= state.limits[kind.index()]
        {
            return Err(rejected(Reason::Capacity));
        }
        let id = state
            .next
            .checked_add(1)
            .ok_or_else(|| rejected(Reason::Capacity))?;
        state.next = id;
        state.records.insert(
            id,
            Record {
                kind,
                entered: false,
                retiring: false,
                caller: None,
                business_failure: None,
                carrier: None,
                carrier_owner: None,
                additional_carriers: Vec::new(),
                carrier_conflict: false,
            },
        );
        Ok(NativePermit {
            owner: self.clone(),
            id,
        })
    }
    fn stop(&self) {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .closing = true;
    }
}
#[derive(Clone)]
pub(crate) struct NativeRegistration {
    owner: Weak<Mutex<State>>,
    id: u64,
}
mod business;
mod carrier;
mod registration;
impl NativePermit {
    pub(crate) fn registration(&self) -> NativeRegistration {
        NativeRegistration {
            owner: Arc::downgrade(&self.owner.0),
            id: self.id,
        }
    }
    pub(crate) fn bind_caller(
        &self,
        caller: Arc<crate::runtime::inbound_owners::HostPeerBinding>,
    ) -> Result<(), ChannelError> {
        let mut state = self
            .owner
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let record = state
            .records
            .get_mut(&self.id)
            .ok_or_else(|| rejected(Reason::Revoked))?;
        if record.caller.is_some() || record.entered {
            return Err(rejected(Reason::InvalidConfig));
        }
        record.caller = Some(caller);
        Ok(())
    }
    /// Synchronously record Unaccounted immediately before the actual legacy
    /// gateway. Completion, cancellation, TaskExit and Drop do not remove it.
    pub(crate) fn enter(&mut self) -> Result<(), ChannelError> {
        let mut state = self
            .owner
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if state.closing {
            return Err(rejected(Reason::Revoked));
        }
        let record = state
            .records
            .get_mut(&self.id)
            .ok_or_else(|| rejected(Reason::Revoked))?;
        if record.entered {
            return Err(rejected(Reason::InvalidConfig));
        }
        record.entered = true;
        Ok(())
    }
    pub(crate) fn retain_error<E: std::error::Error + Send + Sync + 'static>(
        &self,
        error: E,
    ) -> std::io::Error {
        let source: Source = Arc::new(error);
        let mut state = self
            .owner
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.first_source.get_or_insert_with(|| source.clone());
        std::io::Error::other(SharedSource(source))
    }
}
impl Drop for NativePermit {
    fn drop(&mut self) {
        let mut state = self
            .owner
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(record) = state.records.get_mut(&self.id) {
            if record.entered {
                record.retiring = true;
            } else {
                // This exact reserved operation never entered its gateway.
                // No future or error-kind inference and no native-close claim.
                state.records.remove(&self.id);
            }
        }
    }
}
#[derive(Debug)]
struct SharedSource(Source);
impl std::fmt::Display for SharedSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self.0.as_ref(), formatter)
    }
}
impl std::error::Error for SharedSource {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
impl ResourceLifecycle for NativeObligations {
    async fn start(&mut self, _: Instant) -> Result<(), ChannelError> {
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {
        self.stop();
    }
    async fn shutdown(&mut self, _: Instant) -> Result<CloseReport, ChannelError> {
        self.stop();
        // Clone only bounded registration identities before querying original
        // carrier/caller receipts; no ledger lock surrounds other owners.
        let registrations: Vec<_> = self
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .records
            .keys()
            .map(|id| NativeRegistration {
                owner: Arc::downgrade(&self.0),
                id: *id,
            })
            .collect();
        let locally_closed_carriers = registrations
            .iter()
            .filter(|registration| {
                registration
                    .carrier_completion()
                    .is_some_and(|receipt| receipt.matches(registration))
            })
            .count();
        let state = self.0.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(source) = &state.first_source {
            return Err(rejected(Reason::Io).with_source(SharedSource(source.clone())));
        }
        let mut report = CloseReport::default();
        if !state.records.is_empty() {
            let entered = state
                .records
                .values()
                .filter(|record| record.entered)
                .count();
            let retiring = state
                .records
                .values()
                .filter(|record| record.retiring)
                .count();
            report.unfinished.push(Unfinished::Io(format!(
                "{} raw-IP child obligations ({entered} unaccounted, {retiring} retiring, {locally_closed_carriers} locally confirmed carriers) lack complete gateway closure proof",
                state.records.len()
            )));
        }
        Ok(report)
    }
}
fn rejected(reason: Reason) -> ChannelError {
    ChannelError::new(Stage::Close, reason, Impact::Component)
}

#[cfg(test)]
mod tests;

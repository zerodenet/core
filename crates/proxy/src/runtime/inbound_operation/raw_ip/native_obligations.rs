//! Actual raw-IP child quotas retain every gateway lacking native closure proof.
use crate::runtime::raw_ip::tasks::Source;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, Weak},
};
use tokio::time::Instant;
use zero_platform_tokio::contract::{ChannelError, Impact, Reason, Stage};

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
    scope: Option<zero_platform_tokio::contract::scope::TaskScopeIdentity>,
    closing: bool,
    next: u64,
    limits: [usize; 3],
    records: BTreeMap<u64, Record>,
    first_source: Option<Source>,
}
struct Record {
    kind: ChildKind,
    admission_active: usize,
    business_admitted: bool,
    no_business_admission: bool,
    dns: Option<dns::DnsOriginal>,
    entered: bool,
    retiring: bool,
    caller: Option<Arc<crate::runtime::inbound_owners::HostPeerBinding>>,
    business_failure: Option<Arc<zero_engine::EngineError>>,
    carrier: Option<zero_platform_tokio::controlled_carrier::CarrierAdoption>,
    carrier_owner: Option<zero_platform_tokio::controlled_carrier::CarrierOwnerRegistration>,
    additional_carriers: Vec<zero_platform_tokio::controlled_carrier::CarrierAdoption>,
    carrier_conflict: bool,
    pending_carriers: Vec<pending::Claim>,
    gateways: usize,
    uncovered_gateway: bool,
    datagram: Option<crate::runtime::inbound_owners::PeerBoundDatagramRegistration>,
    receiver_starting: bool,
    receiver_uncertain: Option<zero_platform_tokio::contract::scope::ScopeRetirementObservation>,
    receiver: Option<receiver::Receiver>,
    logical_datagrams: Vec<crate::component::datagram_retirement::DatagramRegistration>,
    protocols: Option<
        zero_traits::packet_metadata::PacketMetadataArc<
            zero_platform_tokio::controlled_carrier::ProtocolOriginalChildren,
        >,
    >,
}
pub(crate) struct NativePermit {
    owner: NativeObligations,
    id: u64,
}

impl NativeObligations {
    pub(super) async fn quiescent(&self, deadline: Instant) -> Result<bool, ChannelError> {
        self.settle_joined(deadline).await?;
        let state = self.0.lock().unwrap_or_else(|error| error.into_inner());
        Ok(state.records.is_empty() && state.first_source.is_none() && !state.closing)
    }

    pub(crate) fn new(tcp: usize, udp: usize, dns: usize) -> Self {
        Self(Arc::new(Mutex::new(State {
            scope: None,
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
            return Err(crate::runtime::raw_ip::tasks::capacity::rejected());
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
                admission_active: 0,
                business_admitted: false,
                no_business_admission: false,
                dns: None,
                entered: false,
                retiring: false,
                caller: None,
                business_failure: None,
                carrier: None,
                carrier_owner: None,
                additional_carriers: Vec::new(),
                carrier_conflict: false,
                pending_carriers: Vec::new(),
                gateways: 0,
                uncovered_gateway: false,
                datagram: None,
                receiver_starting: false,
                receiver_uncertain: None,
                receiver: None,
                logical_datagrams: Vec::new(),
                protocols: None,
            },
        );
        Ok(NativePermit {
            owner: self.clone(),
            id,
        })
    }
    fn stop(&self) {
        let calls = {
            let mut state = self.0.lock().unwrap_or_else(|error| error.into_inner());
            state.closing = true;
            state
                .records
                .values()
                .filter_map(|record| record.dns.as_ref().map(|original| original.call.clone()))
                .collect::<Vec<_>>()
        };
        for call in calls {
            call.cancel();
        }
        // Clone one existing finite ledger at a time; no foreign callback or
        // wake runs under this Source's native-obligation state lock.
        let mut previous = 0;
        loop {
            let next = {
                let state = self
                    .0
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state
                    .records
                    .range((
                        std::ops::Bound::Excluded(previous),
                        std::ops::Bound::Unbounded,
                    ))
                    .next()
                    .map(|(id, record)| (*id, record.protocols.clone()))
            };
            let Some((id, children)) = next else { break };
            previous = id;
            if let Some(children) = children {
                children.request_return();
            }
        }
    }
}
#[derive(Clone)]
pub(crate) struct NativeRegistration {
    owner: Weak<Mutex<State>>,
    id: u64,
}
mod business;
mod carrier;
mod completion;
mod dns;
pub(crate) use dns::DnsSourceCaller;
mod admission;
mod logical;
mod pending;
mod protocols;
mod receiver;
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
        let call = if let Some(record) = state.records.get_mut(&self.id) {
            if record.entered {
                record.retiring = true;
                record.dns.as_ref().map(|original| original.call.clone())
            } else {
                // This exact reserved operation never entered its gateway.
                // No future or error-kind inference and no native-close claim.
                state.records.remove(&self.id);
                None
            }
        } else {
            None
        };
        drop(state);
        if let Some(call) = call {
            call.cancel();
        }
    }
}
mod retirement;
use retirement::SharedSource;
fn rejected(reason: Reason) -> ChannelError {
    ChannelError::new(Stage::Close, reason, Impact::Component)
}

#[cfg(test)]
mod tests;

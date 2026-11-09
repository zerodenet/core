//! Cleanup custody of real Packet drivers. This grants no native acquisition,
//! task registration, Source readiness, or native-close authority.
use crate::{
    contract::{
        context::RequestIdentity,
        datagram::BoxDatagramSession,
        lifecycle::{CloseReport, DynChannelControl, Unfinished},
        network::{CarrierContext, UpstreamBinding},
        scope::{TaskJoinWitness, WeakTaskRegistrar},
        ChannelError, CloseReason, DeliveryState, Impact, Reason, Stage,
    },
    controlled_carrier::{loan::DatagramLoan, CarrierAdoption, CarrierOrigin},
};
use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex, OnceLock},
};
use tokio::{sync::Mutex as AsyncMutex, time::Instant};
use zero_core::{
    component::AssociationId,
    request::{ServiceOrigin, ServicePurpose, ServiceRequestFacts},
};
mod actors;
pub(crate) mod attempt;
pub use attempt::PacketDriverPendingCustody as PacketDriverCallRetained;
mod collection;
mod control;
mod faults;
mod handoff;
mod inspection;
mod installation;
mod memory;
mod original;
mod recovery;
pub use memory::{PacketDriverSendBuffers, PacketDriverSendCopy};
mod pending;
mod protocols;
mod registration;
mod retirement;
mod service;
mod source_failures;
pub(crate) mod streams;
pub use service::{GrantedPacketServiceCaller, PacketDriverTarget};
pub use streams::{PacketDriverStreamGrant, PacketDriverStreamRetained};
#[cfg(feature = "native-packet-origin")]
type MetadataRef<T> = zero_traits::packet_metadata::PacketMetadataArc<T>;
#[cfg(not(feature = "native-packet-origin"))]
type MetadataRef<T> = Arc<T>;
#[cfg(feature = "native-packet-origin")]
type SourceTable<T> = zero_tun::packet_memory::PacketMetadataVec<T>;
#[cfg(not(feature = "native-packet-origin"))]
type SourceTable<T> = Vec<T>;
type RecordRef = MetadataRef<Record>;
type InstalledRef = MetadataRef<Installed>;
type ReconnectRef = MetadataRef<actors::Reconnect>;
type ActorFailuresRef = MetadataRef<faults::ActorFailures>;
type SettledRef = MetadataRef<std::sync::atomic::AtomicBool>;
#[cfg(feature = "native-packet-origin")]
type RecordWeak = zero_tun::packet_memory::PacketMetadataWeak<Record>;
#[cfg(not(feature = "native-packet-origin"))]
type RecordWeak = std::sync::Weak<Record>;

/// Retained by the existing HostResources or actual native Source owner.
/// The finite limits describe that owner's real driver/callee graph, not a
/// second IO pool. Removing an attempt requires its own native/users/ACK facts;
/// the original actor and whole Scope remain the Source owner's separate debt.
pub struct PacketDriverCalls {
    original: CarrierOrigin,
    max_calls: NonZeroUsize,
    retiring: std::sync::atomic::AtomicBool,
    failures: Mutex<Arc<faults::Delivery>>,
    source_failures: Arc<source_failures::SourceDelivery>,
    native_failure: Arc<source_failures::Forwarded>,
    calls: Mutex<SourceTable<RecordRef>>,
    reconnects: Mutex<SourceTable<ReconnectRef>>,
    #[cfg(feature = "native-packet-origin")]
    copy_workspaces: OnceLock<Arc<[memory::WireCopy]>>,
}
struct Record {
    original: CarrierOrigin,
    caller: TaskJoinWitness,
    binding: UpstreamBinding,
    identity: crate::contract::context::ServiceFactsOwner,
    call: CarrierContext,
    max_pending: usize,
    #[cfg(feature = "native-packet-origin")]
    copies: Arc<[memory::WireCopy]>,
    allowed: Arc<[crate::controlled_carrier::CarrierOwnerRegistration]>,
    packet_originals: Arc<[CarrierOrigin]>,
    protocol_targets: Option<(
        Arc<[PacketDriverTarget]>,
        zero_core::component::RoleInstanceId,
    )>,
    protocols: Option<crate::controlled_carrier::ProtocolOriginalChildren>,
    streams: Mutex<Vec<Arc<streams::RetainedStream>>>,
    observer: OnceLock<crate::contract::network::UpstreamTransport>,
    upstream: OnceLock<crate::contract::network::PendingCarrierObserver>,
    pending: Mutex<Vec<Arc<original::PendingOriginal>>>,
    installed: OnceLock<InstalledRef>,
    #[cfg(feature = "native-packet-origin")]
    installed_metadata: Mutex<Option<zero_traits::packet_metadata::PacketMetadataCredit>>,
    failures: Arc<faults::RecordFailures>,
    delivery: Arc<faults::Delivery>,
    source_failures: Arc<source_failures::SourceDelivery>,
    forwarded: Arc<source_failures::Forwarded>,
    settled: SettledRef,
}
struct Installed {
    control: AsyncMutex<crate::controlled_carrier::loan::LoanOwner>,
    origin: CarrierOrigin,
    association: AssociationId,
    receiver: OnceLock<TaskJoinWitness>,
    #[cfg(feature = "native-packet-origin")]
    memory: OnceLock<memory::WireCopy>,
    complete: std::sync::atomic::AtomicBool,
}
/// One reserved, single-install caller. Cloning a public TaskId cannot mint it.
pub struct PacketDriverCallGrant {
    record: RecordRef,
    installed: bool,
}
/// Proof only that the same real record retains the actual logical control.
/// It is never a native-close or whole-Source join receipt.
pub struct PacketDriverDatagramHandoff {
    record: RecordRef,
    installed: InstalledRef,
}
impl PacketDriverCallGrant {
    /// Bind the actual selected Node/prefix opener while its ordinary build
    /// invocation strongly owns it. This record keeps only its weak view.
    /// It grants no IO; the real opener still validates its own admission.
    pub fn bind_upstream(
        &self,
        upstream: &crate::contract::network::UpstreamTransport,
    ) -> Result<(), ChannelError> {
        if upstream.binding() != &self.record.binding || self.record.upstream.get().is_some() {
            return Err(failure(Reason::StalePlan));
        }
        let actual = upstream.packet_caller(
            &RequestIdentity::Service(self.record.identity.clone().into()),
            &self.record.call,
        )?;
        if !actual.same_registration(&self.record.caller) {
            return Err(failure(Reason::StalePlan));
        }
        self.record
            .upstream
            .set(upstream.pending_carrier_observer())
            .map_err(|_| failure(Reason::StalePlan))
    }

    pub fn pending_observer(&self) -> crate::contract::network::PendingCarrierObserver {
        self.record
            .observer
            .get()
            .expect("installed observer")
            .pending_carrier_observer()
    }

    pub fn identity(&self) -> RequestIdentity {
        RequestIdentity::Service(self.record.identity.clone().into())
    }
    pub fn call(&self) -> CarrierContext {
        self.record.call.clone()
    }
    pub fn binding(&self) -> &UpstreamBinding {
        &self.record.binding
    }
    pub fn caller(&self) -> &TaskJoinWitness {
        &self.record.caller
    }
    pub fn original(&self) -> &CarrierOrigin {
        &self.record.original
    }
}

impl PacketDriverDatagramHandoff {
    pub fn request_retirement(&self) {
        self.record.request_retirement();
    }
    pub fn confirm_receiver(&self, actual: &TaskJoinWitness) -> Result<(), ChannelError> {
        let valid = if let Some(logical) = self.record.original.logical_packet() {
            logical
                .drivers()
                .iter()
                .any(|driver| driver.same_registration(actual))
        } else {
            self.record
                .original
                .driver()
                .is_some_and(|driver| driver.same_registration(actual))
        };
        if !valid
            || actual.is_joined()
            || !actual.scope_identity().resource_open()
            || !self
                .record
                .installed
                .get()
                .is_some_and(|value| InstalledRef::ptr_eq(value, &self.installed))
        {
            return Err(failure(Reason::StalePlan));
        }
        if let Some(previous) = self.installed.receiver.get() {
            if !previous.same_registration(actual) {
                return Err(failure(Reason::StalePlan));
            }
            self.record.release_host_retained_pending(&self.installed);
            return Ok(());
        }
        #[cfg(feature = "native-packet-origin")]
        self.install_receiver_copy(actual)?;
        self.installed
            .receiver
            .set(actual.clone())
            .map_err(|_| failure(Reason::StalePlan))?;
        self.record.release_host_retained_pending(&self.installed);
        Ok(())
    }
    /// Cleanup may run before a failed candidate could confirm its receiver.
    /// It still requires the original Source's exact already-registered wire;
    /// this grants no IO, caller, native-close or task-completion authority.
    pub fn allows_retirement(&self, actual: &TaskJoinWitness) -> bool {
        match self.installed.receiver.get() {
            Some(receiver) => receiver.same_registration(actual),
            None => match self.record.original.logical_packet() {
                Some(logical) => logical
                    .drivers()
                    .iter()
                    .any(|wire| wire.same_registration(actual)),
                None => self
                    .record
                    .original
                    .driver()
                    .is_some_and(|wire| wire.same_registration(actual)),
            },
        }
    }
    pub fn matches_receiver(&self, actual: &TaskJoinWitness) -> bool {
        self.installed
            .receiver
            .get()
            .is_some_and(|receiver| receiver.same_registration(actual))
    }
    /// Original attempt closure only. The shared Source/ancestor keeps its
    /// independent Scope/native obligations; no task exit is synthesized.
    pub async fn wait_closed(&self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.record.close(deadline).await
    }
    pub fn association(&self) -> AssociationId {
        self.installed.association
    }
    pub fn origin(&self) -> &CarrierOrigin {
        &self.installed.origin
    }
}
fn failure(reason: Reason) -> ChannelError {
    let mut error = ChannelError::new(Stage::Prepare, reason, Impact::Component);
    error.delivery = DeliveryState::NotSubmitted;
    error
}

#[cfg(all(test, feature = "native-packet-origin"))]
#[path = "packet_driver/tests/metadata_observer.rs"]
mod metadata_observer_tests;

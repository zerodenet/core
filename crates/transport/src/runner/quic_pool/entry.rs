use super::{close, setup, QuicPoolProtocol};
use crate::quic::owned::{
    packet::{PacketQueues, PacketSegment},
    DriverDelivery, DriverExitReceiver, OwnedEndpointOwner,
};
use std::sync::{Arc, Mutex};
use tokio::time::Instant;
use zero_platform_tokio::{
    contract::{
        component::{GrantedMetadataListener, ResourceAccess},
        dns::DnsGeneration,
        network::UpstreamBinding,
        scope::{TaskJoinWitness, TaskRegistrar},
        CancellationToken, ChannelError, Impact, Reason, Stage,
    },
    controlled_datagram::metadata_pool::{MetadataPoolOrigin, MetadataPoolPermit},
};

#[derive(Clone, PartialEq, Eq, Hash)]
pub(super) struct Key {
    pub(super) profile: usize,
    pub(super) binding: UpstreamBinding,
    pub(super) dns_generation: Option<DnsGeneration>,
}

pub(super) struct Entry<P: QuicPoolProtocol> {
    pub(super) resources: Arc<dyn ResourceAccess>,
    pub(super) initialization: tokio::sync::Mutex<()>,
    pub(super) cancel: CancellationToken,
    pub(super) state: tokio::sync::Mutex<EntryState<P>>,
    pub(super) touched: Mutex<Instant>,
    pub(super) protocol_wait: Mutex<Option<close::ProtocolWait>>,
}

pub(super) struct EntryState<P: QuicPoolProtocol> {
    pub(super) attempted: bool,
    pub(super) initializing: bool,
    pub(super) setup_worked: bool,
    pub(super) setup_failure: Option<setup::SetupFailure>,
    pub(super) setup_registration:
        Option<zero_platform_tokio::contract::scope::ScopeRetirementObservation>,
    pub(super) setup_exit: Option<zero_platform_tokio::contract::scope::TaskJoined>,
    pub(super) tasks: Option<TaskRegistrar>,
    pub(super) caller: Option<TaskJoinWitness>,
    pub(super) owner: Option<OwnedEndpointOwner>,
    pub(super) receiver: Option<DriverExitReceiver>,
    pub(super) received: Vec<DriverDelivery>,
    pub(super) pending: Option<P::Pending>,
    pub(super) session: Option<Arc<P::Session>>,
    pub(super) permit: Option<MetadataPoolPermit>,
    pub(super) origin: Option<MetadataPoolOrigin>,
    pub(super) protocol_origin: Option<MetadataPoolOrigin>,
    pub(super) anchor_permit: Option<MetadataPoolPermit>,
    pub(super) anchor_receipt: Option<
        zero_platform_tokio::controlled_datagram::metadata::users::MetadataUsersClosedReceipt,
    >,
    pub(super) previous_permit: Option<MetadataPoolPermit>,
    pub(super) retirement:
        Option<zero_platform_tokio::controlled_carrier::CarrierProtocolRetirement>,
    pub(super) original_stop: Option<CancellationToken>,
    pub(super) protocol_closed: bool,
    pub(super) native_sources:
        Option<zero_platform_tokio::controlled_datagram::metadata_pool::MetadataOriginalSources>,
    pub(super) native_failure:
        Option<Arc<zero_platform_tokio::contract::lifecycle::ResourceFailure>>,
    pub(super) segment: Option<PacketSegment>,
    pub(super) queues: Option<PacketQueues>,
    pub(super) receipt: Option<
        zero_platform_tokio::controlled_datagram::metadata::users::MetadataUsersClosedReceipt,
    >,
    pub(super) retiring_registrations: Option<
        zero_platform_tokio::controlled_datagram::metadata::users::MetadataUsersClosedReceipt,
    >,
    pub(super) unbound: Option<GrantedMetadataListener>,
    pub(super) unstarted: Option<
        zero_platform_tokio::controlled_datagram::metadata::users::UnstartedMetadataCompletion,
    >,
    pub(super) retired_drivers: Vec<DriverDelivery>,
    pub(super) retired_application: Vec<zero_platform_tokio::contract::scope::TaskJoined>,
    pub(super) candidate_failures: Vec<Arc<ChannelError>>,
    pub(super) aux_join: Option<TaskJoinWitness>,
    pub(super) aux_exit: Option<zero_platform_tokio::contract::scope::TaskJoined>,
    pub(super) candidates: Option<zero_platform_tokio::controlled_carrier::CarrierCandidates>,
    pub(super) selected_candidate: Option<usize>,
    pub(super) logical: Option<std::net::SocketAddr>,
    pub(super) next_hop: Option<Instant>,
    pub(super) closed: bool,
}

impl<P: QuicPoolProtocol> Default for EntryState<P> {
    fn default() -> Self {
        Self {
            attempted: false,
            initializing: false,
            setup_worked: false,
            setup_failure: None,
            setup_registration: None,
            setup_exit: None,
            tasks: None,
            caller: None,
            owner: None,
            receiver: None,
            received: Vec::new(),
            pending: None,
            session: None,
            permit: None,
            origin: None,
            protocol_origin: None,
            anchor_permit: None,
            anchor_receipt: None,
            previous_permit: None,
            retirement: None,
            original_stop: None,
            protocol_closed: false,
            native_sources: None,
            native_failure: None,
            segment: None,
            queues: None,
            receipt: None,
            retiring_registrations: None,
            unbound: None,
            unstarted: None,
            retired_drivers: Vec::new(),
            retired_application: Vec::new(),
            candidate_failures: Vec::new(),
            aux_join: None,
            aux_exit: None,
            candidates: None,
            selected_candidate: None,
            logical: None,
            next_hop: None,
            closed: false,
        }
    }
}

#[derive(Debug)]
pub(super) struct OriginalChannel(pub(super) Arc<ChannelError>);

impl std::fmt::Display for OriginalChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for OriginalChannel {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

pub(super) fn error(stage: Stage, reason: Reason) -> ChannelError {
    ChannelError::new(stage, reason, Impact::Session)
}

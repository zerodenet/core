//! Reliable UDP on the same original native/loan slot used by packet carriers.
use super::*;
use entry::{Segment, State};
use zero_platform_tokio::{
    contract::scope::TaskJoinWitness,
    controlled_carrier::{CarrierCandidates, CarrierGrant, CarrierUserView},
    controlled_datagram::metadata_pool::{MetadataIcmpMemory, MetadataPoolPermit},
};
use zero_transport::{
    finalmask::owned::PreparedMasks,
    mkcp::owned::OwnedMkcp,
    quic::owned::{
        packet::{BorrowedPacketSegment, PacketQueues, PacketSegment, PacketSocket},
        OwnedEndpointLimits,
    },
};
pub(super) async fn native(
    generation: &Generation,
    entry: &Entry,
    permit: &MetadataPoolPermit,
    caller: &TaskJoinWitness,
    call: CarrierContext,
    candidates: &CarrierCandidates,
    index: usize,
    peer: std::net::SocketAddr,
    socket: PacketSocket,
    queues: PacketQueues,
    plan: PreparedMasks,
    icmp: Option<MetadataIcmpMemory>,
) -> Result<Ready, ChannelError> {
    let options = &generation.options;
    let io = {
        let mut state = entry.owners.state.lock().await;
        if state.cleanup_complete || entry.owners.stop.is_cancelled() {
            return Err(error(Stage::Prepare, Reason::Cancelled));
        }
        state.native_origin = Some(permit.origin());
        let (wire, grant) =
            candidates.open_metadata_with_deadline(index, None, permit, caller, call.deadline)?;
        state.unbound = Some(grant);
        let icmp = match icmp {
            Some(memory) => Some(generation.resources.bind_outbound_icmp(
                candidates.prepare_icmp(index, None, permit, caller, memory)?,
            )?),
            None => None,
        };
        let limits = OwnedEndpointLimits {
            drivers: n(options.drivers()),
            users: options.users,
        };
        let segment = match PacketSegment::prepare_with_endpoint(
            state.unbound.take().unwrap(),
            queues,
            generation.tasks.clone(),
            wire,
            peer,
            limits,
        ) {
            Ok(segment) => segment,
            Err(rejected) => {
                let rejected = *rejected;
                state.unbound = Some(rejected.grant);
                state.queues = Some(rejected.queues);
                return Err(error(Stage::Prepare, Reason::Io).with_source(rejected.error));
            }
        };
        state.segment = Some(Segment::Native(segment));
        let Some(Segment::Native(segment)) = state.segment.as_mut() else {
            unreachable!()
        };
        let owner = OwnedMkcp::native(
            socket,
            segment,
            generation.tasks.clone(),
            limits,
            options.mkcp.expect("accepted mKCP settings"),
            generation.buffers.clone(),
            &generation.metadata_memory,
        )?;
        state.mkcp = Some(owner);
        let State { segment, mkcp, .. } = &mut *state;
        let Some(Segment::Native(segment)) = segment.as_mut() else {
            unreachable!()
        };
        segment.start()?;
        mkcp.as_mut().unwrap().start(plan, icmp, peer)?
    };
    authenticate(generation, io).await.map(Ready::Stream)
}
pub(super) async fn borrowed(
    generation: &Generation,
    entry: &Entry,
    grant: CarrierGrant,
    view: CarrierUserView,
    caller: &TaskJoinWitness,
    demand: &demand::Demand,
    index: usize,
    peer: std::net::SocketAddr,
    socket: PacketSocket,
    queues: PacketQueues,
    plan: PreparedMasks,
) -> Result<Ready, ChannelError> {
    let candidates = demand.borrowed(generation, &grant, caller).await?;
    let target = candidates.select(index, demand.issuer(generation, &grant, caller)?)?;
    let adoption = grant.adoption();
    let loan = demand
        .upstream
        .borrow_service_datagram_candidate(target)
        .await?;
    // adopt installs the original strong owner in its Root before rejection.
    let io = loan.adopt(grant)?;
    let io = {
        let mut state = entry.owners.state.lock().await;
        if state.cleanup_complete || entry.owners.stop.is_cancelled() {
            return Err(error(Stage::Prepare, Reason::Cancelled));
        }
        let segment = match BorrowedPacketSegment::prepare_protocol(
            io,
            view,
            queues,
            generation.tasks.clone(),
            peer,
        ) {
            Ok(segment) => segment,
            Err(rejected) => {
                let rejected = *rejected;
                drop(rejected.io);
                drop(rejected.view);
                state.queues = Some(rejected.queues);
                return Err(error(Stage::Prepare, Reason::Io).with_source(rejected.error));
            }
        };
        state.segment = Some(Segment::Borrowed(segment));
        let options = &generation.options;
        let limits = OwnedEndpointLimits {
            drivers: n(options.drivers()),
            users: options.users,
        };
        let Some(Segment::Borrowed(segment)) = state.segment.as_mut() else {
            unreachable!()
        };
        let owner = OwnedMkcp::borrowed(
            socket,
            segment,
            generation.tasks.clone(),
            limits,
            options.mkcp.expect("accepted mKCP settings"),
            generation.buffers.clone(),
            &generation.metadata_memory,
        )?;
        state.mkcp = Some(owner);
        let State { segment, mkcp, .. } = &mut *state;
        let Some(Segment::Borrowed(segment)) = segment.as_mut() else {
            unreachable!()
        };
        segment.start()?;
        let origin = adoption.origin(segment.driver().expect("actual borrowed wire driver"))?;
        entry
            .origin
            .set(origin)
            .map_err(|_| error(Stage::Prepare, Reason::StalePlan))?;
        mkcp.as_mut().unwrap().start(plan, None, peer)?
    };
    authenticate(generation, io).await.map(Ready::Stream)
}
async fn authenticate(
    generation: &Generation,
    io: zero_platform_tokio::TcpRelayStream,
) -> Result<zero_platform_tokio::TcpRelayStream, ChannelError> {
    match &generation.options.protocol.material {
        Some(material) => material
            .prepare(generation.resolver.as_deref())
            .await
            .map_err(|source| error(Stage::Handshake, Reason::Io).with_source(source))?
            .connect(io)
            .await
            .map_err(|source| error(Stage::Handshake, Reason::Io).with_source(source)),
        None => Ok(io),
    }
}

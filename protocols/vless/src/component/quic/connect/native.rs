//! Original native metadata and the same packet/endpoint/driver graph.
use super::*;
use crate::component::quic::entry::{Segment, State};
use zero_platform_tokio::{
    contract::scope::TaskJoinWitness,
    controlled_datagram::metadata_pool::{
        MetadataIcmpMemory, MetadataPoolPermit, MetadataPoolPreparation,
    },
};
use zero_transport::quic::owned::{
    packet::{PacketSegment, PacketSocket},
    OwnedEndpointLimits, OwnedEndpointOwner,
};
pub(super) fn spawn(
    generation: PacketMetadataArc<Generation>,
    entry: PacketMetadataArc<Entry>,
    preparation: MetadataPoolPreparation,
    upstream: UpstreamTransport,
    call: CarrierContext,
    demand: PacketMetadataArc<super::demand::Demand>,
    index: usize,
    ready: oneshot::Sender<Result<zero_platform_tokio::TcpRelayStream, ChannelError>>,
) -> Result<(), ChannelError> {
    let original = preparation.origin();
    let registration = entry::actor::RegistrationGuard::new(
        entry.clone(),
        entry::actor::OriginalRegistration::Native(original.clone()),
    );
    let (begin, input) = oneshot::channel::<(MetadataPoolPermit, TaskJoinWitness)>();
    let work_entry = entry.clone();
    let actor_generation = generation.clone();
    let tasks = generation.tasks.clone();
    let spawned = preparation.spawn_auxiliary(&tasks, async move {
        let (permit, caller) = input
            .await
            .map_err(|source| error(Stage::Prepare, Reason::Internal).with_source(source))?;
        let work = initialize(
            &actor_generation,
            &work_entry,
            &permit,
            &caller,
            upstream,
            call,
            &demand,
            index,
        );
        let result = tokio::select! {biased;
            _=work_entry.owners.stop.cancelled()=>None,
            result=work=>Some(result),
        };
        match result {
            Some(Ok(io)) => {
                if ready.send(Ok(io)).is_err() {
                    work_entry.retirement.request_retirement();
                }
                Ok(())
            }
            Some(Err(source)) => {
                // Synchronous persist-before-return: a later outer host stop
                // cannot drop a result already produced by this real work.
                let source = work_entry.owners.receive_setup(source);
                let _ = ready.send(Err(ChannelError::retained(source.clone())));
                work_entry.retirement.request_retirement();
                Err(ChannelError::retained(source))
            }
            None => {
                let _ = ready.send(Err(error(Stage::Prepare, Reason::Cancelled)));
                work_entry.retirement.request_retirement();
                Ok(())
            }
        }
    });
    let (permit, caller) = match spawned {
        Ok(actual) => actual,
        Err(source) => {
            registration.finish(original.initializer_registration());
            entry.retirement.request_retirement();
            return Err(source);
        }
    };
    registration.finish(Some(caller.clone()));
    if begin.send((permit, caller)).is_err() {
        entry.retirement.request_retirement();
        return Err(error(Stage::Prepare, Reason::Revoked));
    }
    Ok(())
}
async fn initialize(
    generation: &Generation,
    entry: &Entry,
    permit: &MetadataPoolPermit,
    caller: &TaskJoinWitness,
    _upstream: UpstreamTransport,
    call: CarrierContext,
    demand: &super::demand::Demand,
    index: usize,
) -> Result<zero_platform_tokio::TcpRelayStream, ChannelError> {
    let options = &generation.options;
    let candidates = demand.native(generation).await?;
    let peer = candidates
        .peer(index)
        .ok_or_else(|| error(Stage::Prepare, Reason::InvalidConfig))?;
    let plan = zero_transport::finalmask::owned::PreparedMasks::new(
        options.plan.owned_quic_masks(),
        false,
        generation.buffers.clone(),
        generation.metadata_memory.clone(),
    )
    .map_err(|source| error(Stage::Prepare, Reason::Capacity).with_source(source))?;
    let (socket, queues) =
        PacketSocket::prepare_with_budget_for_masks(peer, generation.buffers.clone(), &plan)
            .map_err(|source| error(Stage::Prepare, Reason::Capacity).with_source(source))?;
    let icmp = options
        .requires_icmp()
        .then(|| MetadataIcmpMemory::reserve(&generation.metadata_memory))
        .transpose()?;
    let client = options
        .protocol
        .material
        .as_ref()
        .expect("same actual QUIC material")
        .prepare_quic(
            generation.resolver.as_deref(),
            &options.profile.alpn_protocols(),
        )
        .await
        .map_err(|source| error(Stage::Prepare, Reason::InvalidConfig).with_source(source))?;
    // Installing the actual native grant and its unique rejected claim is
    // synchronous under the entry lock, before any endpoint future may suspend.
    let handle = {
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
        let segment = match PacketSegment::prepare(
            state.unbound.take().unwrap(),
            queues,
            generation.tasks.clone(),
            wire,
            peer,
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
        let drivers =
            2 + zero_transport::finalmask::owned::driver_count(options.plan.owned_quic_masks());
        let (owner, receiver) = OwnedEndpointOwner::new_packet(
            socket,
            generation.tasks.clone(),
            OwnedEndpointLimits {
                drivers: n(drivers),
                users: options.users,
            },
        );
        state.endpoint = Some(owner);
        state.receiver = Some(receiver);
        let State {
            endpoint, segment, ..
        } = &mut *state;
        let endpoint = endpoint.as_mut().unwrap();
        let Some(Segment::Native(segment)) = segment.as_mut() else {
            unreachable!()
        };
        endpoint.bind_packet_segment(segment)?;
        endpoint.start_prepared_masks(plan, icmp)?;
        segment.start()?;
        endpoint.start(Default::default(), None, Some(client))?
    };
    let connected = zero_platform_tokio::contract::context::with_deadline(
        call.deadline,
        handle.connect(
            peer,
            options
                .profile
                .server_name
                .as_deref()
                .unwrap_or(&options.protocol.server),
        ),
    )
    .await
    .map_err(|source| error(Stage::Handshake, Reason::Timeout).with_source(source))?;
    let connection = match connected {
        Ok(connection) => connection,
        Err(source) => {
            entry.owners.state.lock().await.retry_connection =
                super::demand::connection_failure(&source);
            return Err(source);
        }
    };
    Ok(zero_platform_tokio::TcpRelayStream::new(
        connection.into_bi_stream().await?,
    ))
}

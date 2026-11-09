//! Borrow one exact selected datagram original, never a replacement UDP dial.
use super::*;
use crate::component::quic::entry::{Segment, State};
use zero_platform_tokio::{
    contract::scope::TaskJoinWitness,
    controlled_carrier::{CarrierGrant, CarrierUserView},
};
use zero_transport::quic::owned::{
    packet::{BorrowedPacketSegment, PacketSocket},
    OwnedEndpointLimits, OwnedEndpointOwner,
};
pub(super) fn spawn(
    generation: PacketMetadataArc<Generation>,
    entry: PacketMetadataArc<Entry>,
    mut grant: CarrierGrant,
    upstream: UpstreamTransport,
    call: CarrierContext,
    demand: PacketMetadataArc<super::demand::Demand>,
    index: usize,
    ready: oneshot::Sender<Result<super::Ready, ChannelError>>,
) -> Result<(), ChannelError> {
    // This is the old XICMP-over-datagram-detour boundary, not a restriction on
    // other legitimate stateful masks. The original ICMP native route has no
    // raw arbitrary-interface capability through a lower protocol datagram.
    if generation.options.requires_icmp() {
        entry.owners.set_actor(None);
        entry.retirement.request_retirement();
        return Err(error(Stage::Prepare, Reason::Unsupported));
    }
    let view = if generation.options.h3.is_some() {
        // This actual declaration is ControlledCarrierPool{datagram:true};
        // its unique physical IO is held by the original packet/H3 graph.
        // Plain ServiceDatagram keeps its original taskless transferred view.
        grant.claim_driver_view()
    } else {
        grant.claim_transferred_view()
    }
    .map_err(|source| {
        entry.owners.set_actor(None);
        entry.retirement.request_retirement();
        source
    })?;
    let adoption = grant.adoption();
    let registration = entry::actor::RegistrationGuard::new(
        entry.clone(),
        entry::actor::OriginalRegistration::Borrowed(adoption.clone()),
    );
    let (begin, input) = oneshot::channel::<(CarrierGrant, CarrierUserView, TaskJoinWitness)>();
    let work_entry = entry.clone();
    let actor_generation = generation.clone();
    let spawned = adoption.spawn_auxiliary(&generation.tasks, async move {
        let (grant, view, caller) = input
            .await
            .map_err(|source| error(Stage::Prepare, Reason::Internal).with_source(source))?;
        let work = initialize(
            &actor_generation,
            &work_entry,
            grant,
            view,
            &caller,
            upstream,
            call,
            &demand,
            index,
        );
        let result = tokio::select! {biased;
            _=work_entry.owners.stop.cancelled()=>None,
            _=demand.initializer_cancel.cancelled()=>None,
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
    let (_, caller) = match spawned {
        Ok(actual) => actual,
        Err(source) => {
            registration.finish(adoption.initializer_registration());
            entry.retirement.request_retirement();
            return Err(source);
        }
    };
    registration.finish(Some(caller.clone()));
    if begin.send((grant, view, caller)).is_err() {
        entry.retirement.request_retirement();
        return Err(error(Stage::Prepare, Reason::Revoked));
    }
    Ok(())
}
async fn initialize(
    generation: &Generation,
    entry: &Entry,
    grant: CarrierGrant,
    view: CarrierUserView,
    caller: &TaskJoinWitness,
    _upstream: UpstreamTransport,
    call: CarrierContext,
    demand: &super::demand::Demand,
    index: usize,
) -> Result<super::Ready, ChannelError> {
    let options = &generation.options;
    let candidates = demand.borrowed(generation, &grant, caller).await?;
    let peer = candidates.peer(index)?;
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
    let mut client = options
        .protocol
        .material
        .as_ref()
        .unwrap()
        .prepare_quic(generation.resolver.as_deref(), &options.alpn())
        .await
        .map_err(|source| error(Stage::Prepare, Reason::InvalidConfig).with_source(source))?;
    if let Some((_, profile)) = &options.h3 {
        zero_transport::quic::set_http_keepalive(
            &mut client,
            profile.options.xmux.h_keep_alive_period,
        );
    }
    let issuer = demand.issuer(generation, &grant, caller)?;
    let target = candidates.select(index, issuer)?;
    let adoption = grant.adoption();
    let loan = demand
        .upstream
        .borrow_service_datagram_candidate(target)
        .await?;
    // Original strong LoanOwner is installed by adopt before any rejection.
    let io = loan.adopt(grant)?;
    let handle = {
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
        let drivers = options.drivers();
        let Some(Segment::Borrowed(segment)) = state.segment.as_mut() else {
            unreachable!()
        };
        let (owner, receiver) = OwnedEndpointOwner::new_borrowed_packet(
            socket,
            segment,
            generation.tasks.clone(),
            OwnedEndpointLimits {
                drivers: n(drivers),
                users: options.users,
            },
        )
        .map_err(|rejected| rejected.error)?;
        state.endpoint = Some(owner);
        state.receiver = Some(receiver);
        let State {
            endpoint, segment, ..
        } = &mut *state;
        let endpoint = endpoint.as_mut().unwrap();
        let Some(Segment::Borrowed(segment)) = segment.as_mut() else {
            unreachable!()
        };
        endpoint.start_prepared_masks(plan, None)?;
        segment.start()?;
        let origin = adoption.origin(
            segment
                .driver()
                .expect("actual unique packet driver installed"),
        )?;
        entry
            .origin
            .set(origin)
            .map_err(|_| error(Stage::Prepare, Reason::StalePlan))?;
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
    super::ready::connected(generation, entry, connection).await
}

//! Standard Packet routing owns the stack, policy, accounting and task graphs.
use super::observation::HostObserver;
use super::statistics::IngressTraffic;
use super::*;
use std::time::Instant;
use zero_platform_tokio::contract::{
    context::{HandshakeContext, SessionIdentitySource},
    inbound::{AcceptedRequest, PendingInbound, ReplyOutcome},
    packet::{self},
    ChannelError, Impact, Reason, Stage,
};
use zero_stack::{FragmentReassembler, UserNetworkStack};
mod send;
struct NoBusinessIdentity;
impl SessionIdentitySource for NoBusinessIdentity {
    fn allocate(&self) -> Result<zero_core::component::SessionId, ChannelError> {
        Err(ChannelError::new(
            Stage::Handshake,
            Reason::InvalidConfig,
            Impact::Packet,
        ))
    }
}
pub(in crate::runtime) async fn run(
    pending: PendingInbound,
    context: PacketSourceContext,
    runtime: InboundListenerRuntime,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), EngineError> {
    if !pending
        .packet_listener()
        .is_some_and(|actual| actual.same_listener(&context.original))
    {
        return Err(
            std::io::Error::other("packet source differs from its original listener").into(),
        );
    }
    let children = context.children;
    let route = runtime.route_factory();
    let inbound = route.for_connection(None).inbound_instance()?;
    let cancel = runtime
        .host_control()
        .ok_or_else(|| std::io::Error::other("packet source has no actual host"))?
        .cancellation();
    let request = pending
        .complete(HandshakeContext {
            accepted: None,
            connection: None,
            inbound,
            identities: Arc::new(NoBusinessIdentity),
            deadline: tokio::time::Instant::now() + Duration::from_secs(30),
            cancel: cancel.clone(),
        })
        .await
        .map_err(std::io::Error::other)?;
    let AcceptedRequest::Packet(accepted) = request else {
        return Err(std::io::Error::other("endpoint source did not hand off Packet").into());
    };
    let link_mtu = accepted
        .link_mtu()
        .ok_or_else(|| std::io::Error::other("packet source omitted link MTU"))?;
    let facts = accepted.facts().clone();
    if facts.inbound != inbound
        || facts.endpoint.0 != inbound.component.0
        || facts.subject.is_some()
        || facts.peer.is_some()
    {
        return Err(std::io::Error::other("packet facts differ from actual host endpoint").into());
    }
    let (wire_ready, route_ready) = match runtime.readiness() {
        Some(ready) => {
            let mut parts = ready.split(2)?.into_iter();
            (parts.next(), parts.next())
        }
        None => (None, None),
    };
    let observer = Arc::new(HostObserver {
        traffic: Mutex::new(Arc::new(IngressTraffic::pending())),
        route: route.clone(),
        ready: wire_ready,
    });
    let Some(session) = accepted
        .respond_observed(ReplyOutcome::LocalChannelReady, observer.clone())
        .await
        .map_err(std::io::Error::other)?
    else {
        return Ok(());
    };
    let (sender, mut packet_rx) = send::Sender::take(session);
    let mut sender = Some(sender);
    let mut sending: Option<send::Pending> = None;
    let (response_tx, mut responses) = mpsc::channel::<zero_traits::PacketBuffer>(256);
    let response_tx: zero_stack::packet_output::PacketSender = response_tx.into();
    let (tcp, udp) = UserNetworkStack::new_with_packet_output_and_task_owner(
        response_tx.clone(),
        zero_stack::tcp_mss_for_mtu(link_mtu),
        Arc::new(super::stack_tasks::StackTasks(
            children.stack_workers.clone(),
        )),
    )
    .into_parts();
    let mut tasks = children
        .drivers
        .lock()
        .await
        .map_err(std::io::Error::other)?;
    tasks
        .spawn(tcp::accept(
            tcp.clone(),
            route.clone(),
            children
                .connections
                .clone()
                .ok_or_else(|| std::io::Error::other("packet child group absent"))?,
            children
                .host
                .clone()
                .ok_or_else(|| std::io::Error::other("packet host absent"))?,
            children.native.clone(),
        ))
        .map_err(std::io::Error::other)?;
    tasks
        .spawn(crate::inbound::tun::udp::run_with_owned_tasks(
            runtime.udp_runtime(),
            udp.clone(),
            route.inbound_tag().to_owned(),
            children.udp.clone(),
        ))
        .map_err(std::io::Error::other)?;
    let echo = IcmpEchoRelay::new_owned(
        response_tx.clone(),
        route.clone(),
        shutdown.clone(),
        children.echoes.clone(),
        children.icmp.clone(),
    );
    let ingress_id = crate::runtime::packet_route::next_ingress_id();
    let mut pins = route.packet_statistics_pins();
    let mut fragments = FragmentReassembler::new();
    let mut device_generation = None;
    let mut maintenance = tokio::time::interval(Duration::from_secs(5));
    maintenance.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut cleanup = tokio::time::interval(Duration::from_secs(30));
    cleanup.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let outcome=crate::runtime::inbound_owners::InboundReady::poll_optional(route_ready,async { loop {
        if cancel.is_cancelled() || *shutdown.borrow() { break Ok(()); }
        tokio::select! {
            _=cancel.cancelled()=>break Ok(()),
            changed=shutdown.changed()=>if changed.is_err()||*shutdown.borrow() {break Ok(())},
            received=packet::recv(packet_rx.as_mut())=>{
                let Some(received)=received.map_err(std::io::Error::other)? else { break Ok(()) };
                if received.return_association.endpoint!=facts.endpoint || received.return_association.generation!=facts.inbound.generation {return Err(std::io::Error::other("packet belongs to another endpoint generation").into());}
                if device_generation!=Some(received.return_association.correlation) { fragments=FragmentReassembler::new();device_generation=Some(received.return_association.correlation); }
                let ingress=received.ingress().ok_or_else(||std::io::Error::other("packet source omitted endpoint facts"))?;
                let peer=received.return_association.peer.as_ref().map(|id|id.0.clone());let traffic=observer.traffic.lock().unwrap().clone();
                super::route::feed_inner_packet(received.into_payload(),ingress.local_destination,&traffic,peer,ingress.mtu,&tcp,&udp,&response_tx,&echo,&route,ingress_id,&mut pins,&mut fragments).await;
            }
            response=responses.recv(), if sender.is_some()=>{
                let Some(response)=response else {return Err(std::io::Error::other("packet response channel ended").into());};
                let response=zero_core::packet::IpPacket::new(response,zero_core::packet::ReturnAssociation { endpoint:facts.endpoint,generation:facts.inbound.generation,peer:None,correlation:0 }).map_err(|_|std::io::Error::other("invalid routed IP response"))?;
                sending=Some(sender.take().expect("idle original Packet sender").send(response));
            }
            completed=async { sending.as_mut().expect("pending original write").await }, if sending.is_some()=>{
                let (returned,result)=completed;sending=None;sender=Some(returned);
                if let Err(failure)=result {
                    // An explicit host stop can race the original writer's
                    // revocation. Preserve genuine I/O and Unknown failures.
                    if (cancel.is_cancelled() || *shutdown.borrow()) && failure.failure.error.reason==Reason::Revoked && failure.failure.delivery==zero_platform_tokio::contract::DeliveryState::NotSubmitted { break Ok(()); }
                    return Err(std::io::Error::other(failure.failure).into());
                }
            }
            _=pins.management_changed()=>{route.retain_admitted_packet_pins(&mut pins);pins.expire();}
            _=maintenance.tick()=>{fragments.cleanup_expired(Instant::now());route.retain_admitted_packet_pins(&mut pins);}
            _=cleanup.tick()=>tcp.cleanup_idle(Duration::from_secs(300)).await,
            finished=tasks.join_next()=>break match finished {Some(Ok(result))=>result,Some(Err(error))=>Err(std::io::Error::other(children.drivers.remember(error)).into()),None=>Err(std::io::Error::other("packet stack drivers ended").into())},
        }
    }}).await;
    if let Err(error) = &outcome {
        tracing::debug!(?error, "standard Packet endpoint routing failed");
    }
    drop(sending);
    drop(sender);
    drop(packet_rx);
    children.drivers.finish(&mut tasks).await;
    // The host observer joins the Critical wire task; waiting for its witness
    // here would deadlock that same Scope. Native/joins remain host gates.
    outcome
}

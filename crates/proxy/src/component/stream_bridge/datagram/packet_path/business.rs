//! Original admitted Kernel callers use the same standard Datagram open/finalizer.
use super::*;
use zero_platform_tokio::contract::scope::TaskJoinWitness;

pub(super) fn caller(
    execution: &crate::protocol_registry::TcpExecutionServices,
) -> Result<TaskJoinWitness, ChannelError> {
    let invocation = execution.invocation.as_ref().ok_or_else(|| {
        invalid(
            Reason::InvalidConfig,
            "Packet path has no original invocation",
        )
    })?;
    #[cfg(feature = "raw-ip-runtime")]
    if let Some(actual) = &invocation.caller_native {
        actual.check_open()?;
        return actual
            .caller_witness()
            .ok_or_else(|| invalid(Reason::Revoked, "original raw caller disappeared"));
    }
    if let Some(actual) = &invocation.caller_accepted {
        actual.check_open()?;
        return actual
            .task_join_witness()
            .ok_or_else(|| invalid(Reason::Revoked, "original accepted caller disappeared"));
    }
    Err(invalid(
        Reason::Unsupported,
        "Packet path has no actual admitted Scope caller",
    ))
}

pub(super) fn opening(
    execution: &crate::protocol_registry::TcpExecutionServices,
    roles: Vec<zero_core::component::RoleInstanceId>,
) -> Result<
    (
        crate::component::datagram_retirement::DatagramOpening,
        crate::component::datagram_retirement::DatagramRegistration,
    ),
    ChannelError,
> {
    let actual = caller(execution)?;
    let reservation = execution.reserve_datagram()?;
    let registration = reservation.registration(&actual)?;
    #[cfg(feature = "raw-ip-runtime")]
    if let Some(native) = execution
        .invocation
        .as_ref()
        .and_then(|call| call.caller_native.as_ref())
    {
        native.observe_logical_datagram(registration.clone())?;
    }
    let opening = reservation.begin_open(execution, roles, CancellationToken::new())?;
    Ok((opening, registration))
}

pub(super) async fn open_root(
    execution: &crate::protocol_registry::TcpExecutionServices,
    slot: &Arc<ComponentSlot>,
    tag: &str,
    descriptor: &PacketPathCarrierDescriptor,
    target: Option<&Destination>,
) -> Result<Arc<dyn PacketPathCarrier>, EngineError> {
    let memory = crate::protocol_registry::packet_memory::business::BusinessMemory::from_services(
        execution,
    )?;
    let (opening, registration) = opening(execution, vec![slot.role_id()]).map_err(failure)?;
    let stop = opening
        .pending()
        .upgrade()
        .expect("same retained opening")
        .cancellation();
    let association = opening.association();
    let services = execution.clone().with_pending_datagram(opening.pending());
    let opened = crate::component::stream_bridge::datagram::open_flow(
        &services,
        slot,
        tag,
        super::relay::destination(&descriptor.server, descriptor.port),
        DatagramRequest {
            association,
            targets: target
                .cloned()
                .map_or(DatagramTargets::Multiple, DatagramTargets::Fixed),
        },
    )
    .await
    .map_err(failure)?;
    // The protocol controller is retained in the same pre-I/O Entry before
    // either TX/RX can escape. Cancellation keeps that Entry pollable by Root.
    let mut session = opened.channel;
    session.control = opening.adopt(session.control);
    Ok(Arc::new(Path::new_business(
        session,
        association,
        stop,
        registration,
        memory,
    )))
}

/// Same actual flat relay and independent association lifetime. The protocol
/// sees the original Business facts; the physical prefix uses its existing
/// legitimate background selection and real PendingOpen observer.
pub(super) async fn open_associated(
    services: &PacketPathExecutionServices,
    prefix: &crate::inventory::PreparedTcpRelayPrefix,
    descriptor: &PacketPathCarrierDescriptor,
) -> Result<Arc<dyn PacketPathCarrier>, EngineError> {
    use crate::component::stream_bridge::relay::background::BackgroundPath;
    use zero_platform_tokio::contract::network::CarrierContext;
    let execution = services.tcp();
    caller(&execution).map_err(failure)?;
    let invocation = execution
        .invocation
        .as_ref()
        .expect("checked actual invocation");
    let plan = execution
        .components
        .as_ref()
        .map_err(|source| failure(crate::component::instance::retained(source.clone())))?;
    let (slot, nodes, binding) = super::relay::associated_selection(
        plan,
        prefix,
        descriptor,
        NetworkGeneration(execution.upstream().network_generation()),
    )?;
    plan.authorize_carrier_prefix(slot.instance_id(), &binding.prefix)
        .map_err(failure)?;
    let mut roles = binding.prefix.clone();
    roles.push(slot.role_id());
    let memory = crate::protocol_registry::packet_memory::business::BusinessMemory::from_services(
        &execution,
    )?;
    let (opening, registration) = opening(&execution, roles).map_err(failure)?;
    let pending = opening
        .pending()
        .upgrade()
        .expect("same actual association opening");
    let stop = pending.cancellation();
    let upstream = BackgroundPath {
        nodes,
        binding: binding.clone(),
        endpoint: super::relay::destination(&descriptor.server, descriptor.port),
        wire_endpoint: Destination::new(
            zero_core::Address::Domain(descriptor.server.clone()),
            descriptor.port,
        ),
        final_tag: descriptor
            .tag
            .as_deref()
            .expect("validated associated target"),
        call: CarrierContext {
            session: None,
            deadline: invocation.deadline,
            cancel: stop.clone(),
        },
    }
    .select_observed(
        &execution,
        pending.observer(binding.clone()).map_err(failure)?,
    )
    .await
    .map_err(failure)?;
    let end = invocation
        .deadline
        .unwrap_or_else(|| tokio::time::Instant::now() + std::time::Duration::from_secs(5));
    let role = slot.datagram(end).await.map_err(failure)?;
    let association = opening.association();
    let mut session = role
        .open(
            DatagramRequest {
                association,
                targets: services
                    .packet_target()
                    .cloned()
                    .map_or(DatagramTargets::Multiple, DatagramTargets::Fixed),
            },
            OpenContext {
                identity: invocation.identity.clone(),
                deadline: invocation.deadline,
                cancel: stop.clone(),
                upstream,
                observer: None,
            },
        )
        .await
        .map_err(failure)?;
    session.control = opening.adopt(session.control);
    Ok(Arc::new(Path::new_business(
        session,
        association,
        stop,
        registration,
        memory,
    )))
}

use zero_platform_tokio::contract::{
    component::{BuildContext, ComponentRoles, ComponentSpec, RoleKind},
    lifecycle::BuildError,
    ChannelError, DeliveryState, Impact, Reason, Stage,
};

pub(super) fn validate_ids(spec: &ComponentSpec, context: &BuildContext) -> Result<(), BuildError> {
    let expected: std::collections::BTreeSet<_> = spec.dependencies.iter().copied().collect();
    let actual: std::collections::BTreeSet<_> = context
        .dependencies
        .iter()
        .map(|instance| instance.component)
        .collect();
    if spec.dependencies.len() != context.dependencies.len()
        || actual.len() != context.dependencies.len()
        || actual != expected
    {
        let mut error = invalid("assigned dependency instances do not match the descriptor".into());
        error.component = Some(context.component);
        return Err(error);
    }
    for kind in &spec.roles {
        let present = match kind {
            RoleKind::Stream | RoleKind::Datagram => context.roles.outbound.is_some(),
            RoleKind::Packet => {
                context.roles.outbound.is_some() || context.roles.endpoint.is_some()
            }
            RoleKind::Inbound => context.roles.inbound.is_some(),
            RoleKind::Observation => true,
        };
        if !present {
            return Err(invalid(format!("no assigned identity for {kind:?} role")));
        }
    }
    Ok(())
}

pub(super) fn validate_roles(
    spec: &ComponentSpec,
    roles: &ComponentRoles,
) -> Result<(), BuildError> {
    use zero_platform_tokio::contract::component::ResourceDeclaration;
    let accepted = spec
        .resources
        .iter()
        .filter(|resource| matches!(resource, ResourceDeclaration::AcceptedCarrierSources { .. }))
        .count();
    if accepted != usize::from(roles.accepted_carriers.is_some()) {
        return Err(invalid(
            "accepted Source declaration and original provider differ".into(),
        ));
    }
    if let Some(provider) = &roles.accepted_carriers {
        if spec.task_limit.is_some()
            || !matches!(spec.resources.as_slice(),
                [ResourceDeclaration::AcceptedCarrierSources { max_sources }] if *max_sources == provider.limit())
            || !spec.roles.contains(&RoleKind::Stream)
            || !spec.roles.contains(&RoleKind::Datagram)
            || roles.packet.is_some()
            || roles.inbound.is_some()
        {
            return Err(invalid(
                "accepted Source provider has an unsupported role/resource shape".into(),
            ));
        }
    }
    let actual = [
        (RoleKind::Stream, roles.stream.is_some()),
        (RoleKind::Datagram, roles.datagram.is_some()),
        (RoleKind::Packet, roles.packet.is_some()),
        (RoleKind::Inbound, roles.inbound.is_some()),
        (RoleKind::Observation, roles.observation.is_some()),
    ];
    for (kind, present) in actual {
        if spec.roles.contains(&kind) != present {
            return Err(invalid(format!(
                "built {kind:?} role does not match the descriptor"
            )));
        }
    }
    Ok(())
}

fn invalid(message: String) -> BuildError {
    let mut error = ChannelError::new(Stage::Prepare, Reason::Internal, Impact::Component)
        .with_source(std::io::Error::other(message));
    error.delivery = DeliveryState::NotSubmitted;
    error
}

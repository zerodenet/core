use zero_platform_tokio::contract::{
    component::{BuildContext, ComponentRoles, ComponentSpec, RoleKind},
    lifecycle::BuildError,
    ChannelError, DeliveryState, Impact, Reason, Stage,
};

pub(super) fn validate_ids(spec: &ComponentSpec, context: &BuildContext) -> Result<(), BuildError> {
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

//! All source shapes retain the same installed maintenance and native owner.
use super::*;
pub(super) fn source(
    options: Arc<Options>,
    context: BuildContext,
) -> Result<ComponentInstance, BuildError> {
    let mut prepared = options.prepared.lock().unwrap_or_else(|e| e.into_inner());
    let request = prepared
        .as_ref()
        .ok_or_else(|| ChannelError::new(Stage::Prepare, Reason::Revoked, Impact::Component))?;
    if options.quic.is_none() {
        request.validate_standard_source()?;
    } else {
        request.validate_standard_quic_source()?;
    }
    let request = prepared
        .take()
        .expect("validated original prepared source")
        .activate();
    drop(prepared);
    let maintenance = request.prepare_decryption_maintenance(context.cancel.child_token())?;
    let tasks = context.tasks.clone();
    let instance = carriers(&options, request, context)?;
    Ok(match maintenance {
        Some(actual) => super::maintenance::attach(instance, actual, tasks),
        None => instance,
    })
}
fn carriers(
    options: &Options,
    request: crate::transport::VlessInboundListenerRequest,
    context: BuildContext,
) -> Result<ComponentInstance, BuildError> {
    if let Some(server) = options.quic.clone() {
        return quic::build(
            &options.listen,
            request,
            server,
            options.packet_masks.clone(),
            options.metadata_limits(),
            zero_transport::quic::owned::OwnedEndpointLimits {
                drivers: NonZeroUsize::new(options.quic_drivers()).unwrap(),
                users: options.quic_users,
            },
            context,
        );
    }
    if options.masked_mkcp() {
        return quic::build_mkcp(
            &options.listen,
            request,
            options.packet_masks.clone(),
            options.metadata_limits(),
            options.peer_limits(),
            context,
        );
    }
    if options.udp {
        let (lifecycle, source) = udp::source(
            options.listen.clone(),
            context.resources,
            context.cancel,
            options.peer_limits(),
            std::time::Duration::from_secs(300),
            move |peer| PacketConnection {
                request: request.clone(),
                peer,
            },
        );
        return Ok(ComponentInstance {
            lifecycle: erase_lifecycle(lifecycle),
            roles: ComponentRoles {
                inbound: Some(erase_inbound(source)),
                ..Default::default()
            },
        });
    }
    let resources = context.resources.clone();
    let (lifecycle, source) = tcp::source(
        options.listen.clone(),
        context.resources,
        context.cancel,
        move |socket| Connection {
            request: request.clone(),
            socket,
            resources: resources.clone(),
        },
    );
    Ok(ComponentInstance {
        lifecycle: erase_lifecycle(lifecycle),
        roles: ComponentRoles {
            inbound: Some(erase_inbound(source)),
            ..Default::default()
        },
    })
}

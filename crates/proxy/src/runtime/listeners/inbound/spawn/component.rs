//! A real standard factory/source bypasses the legacy listener execution trait.
use super::*;
use crate::runtime::inbound_owners::{InboundInstallContext, InboundOwner, NativeListenerView};
use std::sync::Arc;

#[allow(clippy::too_many_arguments)]
pub(super) fn spawn(
    recipe: crate::component::ComponentRecipe,
    view: NativeListenerView,
    installed: InboundInstallContext,
    runtime_factory: &InboundListenerRuntimeFactory,
    inbound: &InboundConfig,
    shutdown: watch::Sender<bool>,
    listeners: &mut JoinSet<Result<(), EngineError>>,
    rollback: bool,
) -> Result<Arc<InboundOwner>, EngineError> {
    let owner = installed.owner();
    if !rollback {
        runtime_factory
            .register_preparing(&owner)
            .map_err(|error| owner.retain_managed_listener_error(error))?;
    }
    let limit = match recipe.spec().resources.as_slice() {
        [zero_platform_tokio::contract::component::ResourceDeclaration::TcpListener {
            accepted: Some(limits),
            ..
        }, ..] => limits.max_sockets,
        [zero_platform_tokio::contract::component::ResourceDeclaration::DatagramListener {
            accepted: Some(limits),
            ..
        }, ..] => limits.max_peers,
        [zero_platform_tokio::contract::component::ResourceDeclaration::MetadataDatagram {
            ..
        }, zero_platform_tokio::contract::component::ResourceDeclaration::DatagramAssociations {
            max_associations,
            ..
        }, ..] => *max_associations,
        #[cfg(feature = "raw-ip-runtime")]
        [zero_platform_tokio::contract::component::ResourceDeclaration::DatagramListener {
            accepted: None,
            ..
        }, zero_platform_tokio::contract::component::ResourceDeclaration::NativeIcmp { .. }, zero_platform_tokio::contract::component::ResourceDeclaration::PacketDatagramReads {
            datagram: 0,
            ..
        }] => std::num::NonZeroUsize::new(1).unwrap(),
        _ => {
            return Err(owner.retain_managed_listener_error(
                std::io::Error::other("standard inbound has no accepted task declaration").into(),
            ));
        }
    };
    let (source, peer) = installed
        .build_inbound(
            recipe,
            view,
            Some(
                runtime_factory
                    .for_inbound(inbound.tag.clone())
                    .route_factory()
                    .for_connection(None)
                    .upstream_services(),
            ),
        )
        .map_err(|error| owner.retain_managed_listener_error(error))?;
    let runtime = runtime_factory
        .for_inbound(inbound.tag.clone())
        .with_inbound_instance(owner.identity())
        .with_readiness(owner.readiness())
        .with_datagram_source(peer);
    let tag = inbound.tag.clone();
    let protocol = inbound.protocol.protocol_name();
    owner
        .spawn_driver(async move {
            info!(inbound_tag = %tag, protocol, "standard inbound source started");
            crate::runtime::inbound_operation::source::run(
                source,
                runtime,
                shutdown.subscribe(),
                limit,
                protocol,
            )
            .await
        })
        .map_err(|error| owner.retain_managed_listener_error(error))?;
    owner.seal_listener_installation();
    let observing = owner.clone();
    listeners.spawn(async move {
        observing
            .observe_listener()
            .await
            .map_err(|error| observing.observer_error(error))
    });
    Ok(owner)
}

//! Native source over the original Metadata grant and sealed Quinn users.
use super::*;
pub(super) fn endpoint(d: &Destination) -> std::net::SocketAddr {
    match &d.address {
        zero_core::Address::Ipv4(ip) => std::net::SocketAddr::new((*ip).into(), d.port),
        zero_core::Address::Ipv6(ip) => std::net::SocketAddr::new((*ip).into(), d.port),
        _ => unreachable!("QUIC binder requires a validated literal endpoint"),
    }
}
pub(super) fn build(
    listen: &Destination,
    request: crate::transport::VlessInboundListenerRequest,
    server: zero_transport::quic::QuicServerConfig,
    masks: Vec<zero_transport::finalmask::udp::Mask>,
    metadata: zero_platform_tokio::controlled_datagram::metadata::MetadataLimits,
    endpoint_limits: zero_transport::quic::owned::OwnedEndpointLimits,
    context: BuildContext,
) -> Result<ComponentInstance, BuildError> {
    let resources = context.resources.clone();

    let (lifecycle, source) = zero_transport::quic::source::source(
        MetadataDatagramRequest {
            ordinal: 0,
            source: endpoint(listen),
            limits: metadata,
        },
        server,
        masks,
        endpoint_limits,
        context.resources,
        context.tasks,
        context.cancel,
        move |incoming| Connection {
            incoming,
            request: request.clone(),
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
struct Connection {
    incoming: zero_transport::quic::owned::OwnedIncoming,
    request: crate::transport::VlessInboundListenerRequest,
    resources: Arc<dyn ResourceAccess>,
}
impl InboundHandshake for Connection {
    async fn complete(self, context: HandshakeContext) -> Result<AcceptedRequest, ChannelError> {
        let local = self.incoming.local_addr();
        let connection = self.incoming.establish().await?;
        self.request
            .accept_standard_quic(connection, local, context, self.resources)
            .await
    }
}

pub(super) fn build_mkcp(
    listen: &Destination,
    request: crate::transport::VlessInboundListenerRequest,
    masks: Vec<zero_transport::finalmask::udp::Mask>,
    metadata: zero_platform_tokio::controlled_datagram::metadata::MetadataLimits,
    context: BuildContext,
) -> Result<ComponentInstance, BuildError> {
    let limits = super::peer_limits();
    let (lifecycle, source) = zero_transport::finalmask::owned::source::source(
        MetadataDatagramRequest {
            ordinal: 0,
            source: endpoint(listen),
            limits: metadata,
        },
        masks,
        context.resources,
        context.tasks,
        context.cancel,
        limits.max_peers,
        limits.packets_per_peer,
        move |peer| MaskedPeer {
            request: request.clone(),
            peer,
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
struct MaskedPeer {
    request: crate::transport::VlessInboundListenerRequest,
    peer: zero_transport::finalmask::owned::source::Peer,
}
impl InboundHandshake for MaskedPeer {
    async fn complete(self, context: HandshakeContext) -> Result<AcceptedRequest, ChannelError> {
        self.request
            .accept_standard_mkcp_carrier_peer(self.peer, context)
    }
}

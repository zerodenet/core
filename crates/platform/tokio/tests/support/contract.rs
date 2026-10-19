use std::sync::Arc;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use zero_core::{component::*, request::*, Address, ProtocolType};
use zero_platform_tokio::contract::{
    context::{OpenContext, RequestIdentity},
    datagram::{BoxDatagramSession, DatagramRequest},
    network::{CarrierContext, UpstreamBinding, UpstreamOpener, UpstreamTransport},
    stream::BoxStream,
    BoxFuture, ChannelError,
};

pub fn destination() -> Destination {
    Destination::new(Address::Domain("example.test".into()), 443)
}
pub fn facts() -> RequestFacts {
    RequestFacts {
        session: SessionId(1),
        inbound: RoleId::Inbound(InboundId(1)),
        original_source: None,
        effective_source: None,
        local_endpoint: None,
        original_target: None,
        target: Some(destination()),
        target_source: None,
        subject: None,
        peer: None,
        protocol: ProtocolType::new("contract-test"),
        sni: None,
        process: None,
    }
}

struct MustNotDial;
impl UpstreamOpener for MustNotDial {
    fn open_stream(
        &self,
        _: &UpstreamBinding,
        _: Destination,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        panic!("this local contract fixture must not open a carrier")
    }
    fn open_datagram(
        &self,
        _: &UpstreamBinding,
        _: DatagramRequest,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxDatagramSession, ChannelError>> {
        panic!("this local contract fixture must not open a carrier")
    }
}

pub fn context() -> OpenContext {
    with_identity(RequestIdentity::Business(Arc::new(facts())))
}

pub fn inbound() -> RoleInstanceId {
    RoleInstanceId {
        component: ComponentId(1),
        role: RoleId::Inbound(InboundId(1)),
        generation: ResourceGeneration(1),
    }
}

pub fn packet_context() -> OpenContext {
    with_identity(RequestIdentity::Packet(Arc::new(
        zero_core::packet::PacketFacts {
            endpoint: EndpointId(1),
            inbound: RoleInstanceId {
                component: ComponentId(1),
                role: RoleId::Endpoint(EndpointId(1)),
                generation: ResourceGeneration(2),
            },
            carrier_source: None,
            subject: None,
            peer: None,
        },
    )))
}

fn with_identity(identity: RequestIdentity) -> OpenContext {
    OpenContext {
        identity,
        deadline: Instant::now() + std::time::Duration::from_secs(1),
        cancel: CancellationToken::new(),
        observer: None,
        upstream: UpstreamTransport::new(
            UpstreamBinding {
                prefix: vec![RoleInstanceId {
                    component: ComponentId(7),
                    role: RoleId::Outbound(OutboundId(7)),
                    generation: ResourceGeneration(1),
                }],
                dial: Default::default(),
                network_generation: NetworkGeneration(9),
            },
            Arc::new(MustNotDial),
        ),
    }
}

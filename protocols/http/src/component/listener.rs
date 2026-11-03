//! Protocol-owned HTTP source; binding, task admission and business policy are host-owned.
use alloc::{boxed::Box, format, vec};
use std::num::NonZeroUsize;
use std::sync::Arc;
use zero_core::request::Destination;
use zero_platform_tokio::contract::{
    component::{
        AcceptedTcpLimits, BuildContext, ComponentFactory, ComponentInstance, ComponentRoles,
        ComponentSpec, ResourceDeclaration, ResourceOwner, RoleKind,
    },
    context::{HandshakeContext, InboundConnectionFacts},
    inbound::{erase_inbound, tcp, AcceptedRequest, InboundHandshake},
    lifecycle::{erase_lifecycle, BuildError},
    ChannelError,
};
use zero_platform_tokio::{TcpRelayStream, TokioSocket};

pub struct InboundOptions {
    pub listen: Destination,
    pub accepted: AcceptedTcpLimits,
    pub max_bind_attempts: NonZeroUsize,
}
pub struct InboundFactory;
impl ComponentFactory for InboundFactory {
    type Options = InboundOptions;
    fn describe(&self, options: &InboundOptions) -> ComponentSpec {
        ComponentSpec {
            owner: ResourceOwner::Component,
            roles: vec![RoleKind::Inbound],
            dependencies: vec![],
            static_identity: format!(
                "http:{:?}:{:?}:{}",
                options.listen, options.accepted, options.max_bind_attempts
            )
            .into_bytes()
            .into(),
            resources: vec![ResourceDeclaration::TcpListener {
                destination: options.listen.clone(),
                accepted: Some(options.accepted),
                max_bind_attempts: options.max_bind_attempts,
            }],
            task_limit: None,
        }
    }
    fn build(
        &self,
        options: Arc<InboundOptions>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        let (lifecycle, source) = tcp::source(
            options.listen.clone(),
            context.resources,
            context.cancel,
            Connection::new,
        );
        Ok(ComponentInstance {
            lifecycle: erase_lifecycle(lifecycle),
            roles: ComponentRoles {
                inbound: Some(erase_inbound(source)),
                ..Default::default()
            },
        })
    }
}

struct Connection {
    io: TcpRelayStream,
    facts: InboundConnectionFacts,
}
impl Connection {
    fn new(socket: TokioSocket) -> Self {
        let facts = InboundConnectionFacts::from_stream(&socket);
        Self {
            facts,
            io: TcpRelayStream::from(socket),
        }
    }
}
impl InboundHandshake for Connection {
    async fn complete(self, _: HandshakeContext) -> Result<AcceptedRequest, ChannelError> {
        // This is a real request source, not a business exchange hidden in Control.
        Ok(AcceptedRequest::Multiplex(Box::new(
            super::Requests::new(self.io).with_connection_facts(self.facts),
        )))
    }
}

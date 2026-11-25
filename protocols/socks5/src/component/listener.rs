//! Standard SOCKS5 inbound source backed by [`StandardTcpInboundFactory`].
use crate::transport::{Socks5InboundAcceptor, Socks5InboundUserRef};
use alloc::{boxed::Box, sync::Arc, vec, vec::Vec};
use std::num::NonZeroUsize;
use zero_core::request::Destination;
use zero_platform_tokio::contract::{
    component::*,
    context::{HandshakeContext, InboundConnectionFacts},
    inbound::{AcceptedRequest, InboundHandshake},
    lifecycle::BuildError,
    BoxFuture, ChannelError, Impact, Reason, Stage,
};
use zero_transport::runner::{
    StandardTcpInboundFactory, StandardTcpInboundOptions, StandardTcpInboundProtocol,
};

pub struct Socks5StandardInbound;

impl StandardTcpInboundProtocol for Socks5StandardInbound {
    type Staged = Socks5InboundAcceptor;
    type Active = Socks5InboundAcceptor;

    fn default_work_tasks(_staged: &Self::Staged) -> NonZeroUsize {
        NonZeroUsize::new(2).unwrap()
    }

    fn default_work_resources(_staged: &Self::Staged) -> usize {
        2
    }

    fn extra_resources(options: &StandardTcpInboundOptions<Self>) -> Vec<ResourceDeclaration> {
        vec![ResourceDeclaration::AcceptedDatagram {
            max_sockets: NonZeroUsize::new(1).unwrap(),
            max_pending_sends: options.source_limits().pending_sends,
        }]
    }

    fn activate(staged: Self::Staged) -> Result<Self::Active, ChannelError> {
        Ok(staged)
    }

    fn accept(
        active: Self::Active,
        socket: zero_platform_tokio::TokioSocket,
        resources: Arc<dyn ResourceAccess>,
        context: HandshakeContext,
    ) -> BoxFuture<'static, Result<AcceptedRequest, ChannelError>> {
        let facts = InboundConnectionFacts::from_stream(&socket);
        Box::pin(
            super::inbound::Connection::from_stream(Box::pin(socket), facts, active, resources)
                .complete(context),
        )
    }
}

pub struct InboundOptions(StandardTcpInboundOptions<Socks5StandardInbound>);

impl InboundOptions {
    pub fn new<'a>(
        listen: Destination,
        accepted: AcceptedTcpLimits,
        max_bind_attempts: NonZeroUsize,
        max_pending_sends: NonZeroUsize,
        users: impl IntoIterator<Item = Socks5InboundUserRef<'a>>,
    ) -> Result<Self, BuildError> {
        if accepted.max_tasks_per_socket.get() < 2 || accepted.max_resources_per_socket < 2 {
            return Err(ChannelError::new(
                Stage::Prepare,
                Reason::InvalidConfig,
                Impact::Component,
            ));
        }
        let users: Vec<_> = users.into_iter().collect();
        let mut private_identity = Vec::new();
        for user in &users {
            for field in [
                user.username.as_bytes(),
                user.password.as_bytes(),
                user.principal_key.unwrap_or("").as_bytes(),
            ] {
                private_identity.extend_from_slice(&(field.len() as u64).to_le_bytes());
                private_identity.extend_from_slice(field);
            }
            private_identity.push(u8::from(user.principal_key.is_some()));
            private_identity.extend_from_slice(&user.up_bps.unwrap_or(0).to_le_bytes());
            private_identity.push(u8::from(user.up_bps.is_some()));
            private_identity.extend_from_slice(&user.down_bps.unwrap_or(0).to_le_bytes());
            private_identity.push(u8::from(user.down_bps.is_some()));
        }
        let acceptor = Socks5InboundAcceptor::from_options_refs(users);
        let inner = StandardTcpInboundOptions::new(listen, acceptor, &private_identity)
            .with_accepted_limits(accepted, max_bind_attempts, max_pending_sends);
        Ok(Self(inner))
    }

    pub fn listen(&self) -> &Destination {
        self.0.listen()
    }

    pub fn source_limits(&self) -> InboundSourceLimits {
        self.0.source_limits()
    }

    pub fn with_inbound_source_limits(
        self,
        limits: InboundSourceLimits,
    ) -> Result<Self, ChannelError> {
        self.0.with_inbound_source_limits(limits).map(Self)
    }

    pub fn with_inbound_work_limits(
        self,
        tasks: NonZeroUsize,
        resources: usize,
    ) -> Result<Self, BuildError> {
        self.0
            .with_work_limits_bounded(tasks, resources, 2, 2)
            .map(Self)
    }

    pub fn acceptor(&self) -> Socks5InboundAcceptor {
        self.0
            .map_staged(Clone::clone)
            .expect("cold SOCKS5 staged acceptor")
    }
}

pub struct InboundFactory;

impl ComponentFactory for InboundFactory {
    type Options = InboundOptions;

    fn describe(&self, options: &InboundOptions) -> ComponentSpec {
        StandardTcpInboundFactory::<Socks5StandardInbound>::new().describe(&options.0)
    }

    fn build(
        &self,
        options: Arc<InboundOptions>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        let inner = Arc::new(options.0.take_into_owned()?);
        StandardTcpInboundFactory::<Socks5StandardInbound>::new().build(inner, context)
    }
}

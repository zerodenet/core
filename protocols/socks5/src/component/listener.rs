//! One standard source for both commands, sharing the real TCP accepted owner.
use crate::transport::{Socks5InboundAcceptor, Socks5InboundUserRef};
use alloc::{sync::Arc, vec, vec::Vec};
use std::{
    collections::hash_map::RandomState,
    hash::{BuildHasher, Hash, Hasher},
    num::NonZeroUsize,
    sync::OnceLock,
};
use zero_core::request::Destination;
use zero_platform_tokio::contract::{
    component::*,
    context::InboundConnectionFacts,
    inbound::{erase_inbound, tcp},
    lifecycle::{erase_lifecycle, BuildError},
    ChannelError, Impact, Reason, Stage,
};

pub struct InboundOptions {
    pub listen: Destination,
    pub accepted: AcceptedTcpLimits,
    pub max_bind_attempts: NonZeroUsize,
    pub max_pending_sends: NonZeroUsize,
    acceptor: Socks5InboundAcceptor,
    identity: Arc<[u8]>,
}
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
        // Full credential/rate equality remains private and process-keyed.
        // Descriptions and diagnostics never expose passwords or stable hashes.
        static KEYS: OnceLock<[RandomState; 2]> = OnceLock::new();
        let mut identity = Vec::new();
        for key in KEYS.get_or_init(|| [RandomState::new(), RandomState::new()]) {
            let mut hash = key.build_hasher();
            listen.hash(&mut hash);
            accepted.max_sockets.hash(&mut hash);
            accepted.max_tasks_per_socket.hash(&mut hash);
            accepted.max_resources_per_socket.hash(&mut hash);
            max_bind_attempts.hash(&mut hash);
            max_pending_sends.hash(&mut hash);
            for user in &users {
                (
                    user.username,
                    user.password,
                    user.principal_key,
                    user.up_bps,
                    user.down_bps,
                )
                    .hash(&mut hash);
            }
            identity.extend_from_slice(&hash.finish().to_le_bytes());
        }
        Ok(Self {
            listen,
            accepted,
            max_bind_attempts,
            max_pending_sends,
            acceptor: Socks5InboundAcceptor::from_options_refs(users),
            identity: identity.into(),
        })
    }
    pub fn acceptor(&self) -> Socks5InboundAcceptor {
        self.acceptor.clone()
    }
}
pub struct InboundFactory;
impl ComponentFactory for InboundFactory {
    type Options = InboundOptions;
    fn describe(&self, options: &InboundOptions) -> ComponentSpec {
        ComponentSpec {
            owner: ResourceOwner::Component,
            roles: vec![RoleKind::Inbound],
            dependencies: vec![],
            static_identity: options.identity.clone(),
            resources: vec![
                ResourceDeclaration::TcpListener {
                    destination: options.listen.clone(),
                    accepted: Some(options.accepted),
                    max_bind_attempts: options.max_bind_attempts,
                },
                ResourceDeclaration::AcceptedDatagram {
                    max_sockets: NonZeroUsize::new(1).unwrap(),
                    max_pending_sends: options.max_pending_sends,
                },
            ],
            task_limit: None,
        }
    }
    fn build(
        &self,
        options: Arc<InboundOptions>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        let resources = context.resources.clone();
        let (lifecycle, source) = tcp::source(
            options.listen.clone(),
            context.resources,
            context.cancel,
            move |socket| {
                let facts = InboundConnectionFacts::from_stream(&socket);
                super::inbound::Connection::new(
                    socket.into(),
                    facts,
                    options.acceptor(),
                    resources.clone(),
                )
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
}

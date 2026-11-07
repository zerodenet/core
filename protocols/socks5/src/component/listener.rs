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
    pub fn source_limits(&self) -> InboundSourceLimits {
        InboundSourceLimits {
            accepted_sockets: self.accepted.max_sockets,
            bind_attempts: self.max_bind_attempts,
            pending_sends: self.max_pending_sends,
            ..Default::default()
        }
    }
    pub fn with_inbound_source_limits(
        mut self,
        limits: InboundSourceLimits,
    ) -> Result<Self, ChannelError> {
        let limits = limits.validate()?;
        let mut identity = self.identity.to_vec();
        identity.extend_from_slice(b"actual-source-limits-v1");
        for value in [
            limits.accepted_sockets,
            limits.bind_attempts,
            limits.pending_sends,
        ] {
            identity.extend_from_slice(&value.get().to_le_bytes());
        }
        self.identity = identity.into();
        self.accepted.max_sockets = limits.accepted_sockets;
        self.max_bind_attempts = limits.bind_attempts;
        self.max_pending_sends = limits.pending_sends;
        Ok(self)
    }

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
    pub fn with_inbound_work_limits(
        mut self,
        tasks: NonZeroUsize,
        resources: usize,
    ) -> Result<Self, BuildError> {
        if !(2..=4096).contains(&tasks.get()) || !(2..=65536).contains(&resources) {
            return Err(ChannelError::new(
                Stage::Prepare,
                Reason::InvalidConfig,
                Impact::Component,
            ));
        }
        let mut identity = self.identity.to_vec();
        identity.extend_from_slice(b"actual-inbound-work-limits-v1");
        identity.extend_from_slice(&tasks.get().to_le_bytes());
        identity.extend_from_slice(&resources.to_le_bytes());
        self.identity = identity.into();
        self.accepted.max_tasks_per_socket = tasks;
        self.accepted.max_resources_per_socket = resources;
        Ok(self)
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

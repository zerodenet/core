//! Actual standard source, including one published profile and original task graph.
use crate::transport::{PreparedVmessInboundListenerRequest, VmessInboundListenerRequest};
use std::{
    collections::hash_map::RandomState,
    hash::{BuildHasher, Hash, Hasher},
    num::NonZeroUsize,
    sync::{Arc, Mutex, OnceLock},
};
use zero_core::request::Destination;
use zero_platform_tokio::contract::{
    component::*,
    context::HandshakeContext,
    inbound::*,
    lifecycle::{erase_lifecycle, BuildError},
    ChannelError, Impact, Reason, Stage,
};
pub struct Options {
    listen: Destination,
    request: Mutex<Option<PreparedVmessInboundListenerRequest>>,
    identity: Arc<[u8]>,
}
impl Options {
    pub fn new(
        listen: Destination,
        request: PreparedVmessInboundListenerRequest,
        private_identity: &[u8],
    ) -> Self {
        static KEYS: OnceLock<[RandomState; 2]> = OnceLock::new();
        let mut identity = Vec::new();
        for key in KEYS.get_or_init(|| [RandomState::new(), RandomState::new()]) {
            let mut hash = key.build_hasher();
            listen.hash(&mut hash);
            private_identity.hash(&mut hash);
            identity.extend_from_slice(&hash.finish().to_le_bytes());
        }
        Self {
            listen,
            request: Mutex::new(Some(request)),
            identity: identity.into(),
        }
    }
}
pub struct Factory;
impl ComponentFactory for Factory {
    type Options = Options;
    fn describe(&self, options: &Options) -> ComponentSpec {
        ComponentSpec {
            owner: ResourceOwner::Component,
            roles: vec![RoleKind::Inbound],
            dependencies: vec![],
            static_identity: options.identity.clone(),
            task_limit: None,
            resources: vec![ResourceDeclaration::TcpListener {
                destination: options.listen.clone(),
                max_bind_attempts: NonZeroUsize::new(50).unwrap(),
                accepted: Some(AcceptedTcpLimits {
                    max_sockets: NonZeroUsize::new(4096).unwrap(),
                    // Parent, scoped gRPC carrier, real MUX reader/writer and 256 requests
                    // with an independent actual UDP driver per request.
                    max_tasks_per_socket: NonZeroUsize::new(516).unwrap(),
                    max_resources_per_socket: 257,
                }),
            }],
        }
    }
    fn build(
        &self,
        options: Arc<Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        let prepared = options
            .request
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .ok_or_else(|| ChannelError::new(Stage::Prepare, Reason::Revoked, Impact::Component))?;
        // The real host builds only after StopOld. Cold decode never publishes.
        let request = prepared.activate().map_err(|source| {
            ChannelError::new(Stage::Prepare, Reason::InvalidConfig, Impact::Component)
                .with_source(source)
        })?;
        let (lifecycle, source) = tcp::source(
            options.listen.clone(),
            context.resources,
            context.cancel,
            move |socket| Connection {
                request: request.clone(),
                socket,
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
struct Connection {
    request: VmessInboundListenerRequest,
    socket: zero_platform_tokio::TokioSocket,
}
impl InboundHandshake for Connection {
    async fn complete(self, context: HandshakeContext) -> Result<AcceptedRequest, ChannelError> {
        self.request
            .accept_standard_request(self.socket, context)
            .await
    }
}

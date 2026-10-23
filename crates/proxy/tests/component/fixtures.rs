use std::{num::NonZeroUsize, path::PathBuf, sync::Arc, time::Duration};
use tokio::time::Instant;
use zero_core::{
    component::*,
    request::{Destination, RequestFacts},
    Address, ProtocolType,
};
use zero_platform_tokio::contract::{
    component::{BuildContext, ResourceAccess},
    context::{OpenContext, RequestIdentity},
    lifecycle::BuildError,
    network::{UpstreamBinding, UpstreamOpener, UpstreamTransport},
    packet::BoxPacketSession,
    scope::TaskScope,
    BoxFuture, CancellationToken,
};
use zero_traits::{
    DialPolicy, ProtocolCapabilityDescriptor, ProtocolCapabilityLevel, ProtocolCapabilityState,
    ProtocolNetworkCapability,
};

#[derive(Default)]
pub struct RecordedFacts(
    pub  std::sync::Mutex<
        Vec<(
            Option<SessionId>,
            zero_platform_tokio::contract::context::ConnectFact,
        )>,
    >,
);
impl zero_platform_tokio::contract::context::ConnectObserver for RecordedFacts {
    fn record(
        &self,
        session: Option<SessionId>,
        fact: zero_platform_tokio::contract::context::ConnectFact,
    ) {
        self.0.lock().unwrap().push((session, fact));
    }
}

pub fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}
pub fn destination() -> Destination {
    Destination::new(Address::Ipv4([127, 0, 0, 1]), 1234)
}
pub fn descriptor() -> ProtocolCapabilityDescriptor {
    ProtocolCapabilityDescriptor {
        protocol: "direct",
        feature: "core",
        status: ProtocolCapabilityLevel::Supported,
        compatibility_baseline: "kernel_builtin",
        inbound: ProtocolNetworkCapability::new(
            ProtocolCapabilityState::not_applicable(),
            ProtocolCapabilityState::not_applicable(),
        ),
        outbound: ProtocolNetworkCapability::new(
            ProtocolCapabilityState::supported(),
            ProtocolCapabilityState::not_applicable(),
        ),
        transports: &["tcp"],
        mux: ProtocolCapabilityState::not_applicable(),
        limitations: &[],
    }
}
pub fn build_context() -> (BuildContext, TaskScope) {
    let scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(1).unwrap(),
    );
    let context = BuildContext {
        component: ComponentId(7),
        generation: ResourceGeneration(3),
        dependencies: Arc::from([]),
        roles: ComponentRoleIds {
            outbound: Some(OutboundId(11)),
            ..Default::default()
        },
        resources: Arc::new(NoResources),
        tasks: scope.registrar(),
        cancel: CancellationToken::new(),
        background_carrier: None,
    };
    (context, scope)
}
pub fn open_context(opener: Arc<dyn UpstreamOpener>, dial: DialPolicy) -> OpenContext {
    OpenContext {
        identity: RequestIdentity::Business(Arc::new(RequestFacts {
            session: SessionId(5),
            inbound: RoleId::Inbound(InboundId(9)),
            original_source: None,
            effective_source: None,
            local_endpoint: None,
            original_target: Some(destination()),
            target: Some(destination()),
            target_source: None,
            subject: None,
            peer: None,
            protocol: ProtocolType::new("direct"),
            sni: None,
            process: None,
        })),
        deadline: Some(deadline()),
        cancel: CancellationToken::new(),
        observer: None,
        upstream: UpstreamTransport::new(
            UpstreamBinding {
                prefix: vec![RoleInstanceId {
                    component: ComponentId(4),
                    role: RoleId::Outbound(OutboundId(2)),
                    generation: ResourceGeneration(8),
                }],
                dial,
                network_generation: NetworkGeneration(6),
            },
            opener,
        ),
    }
}

// A test grant that fails immediately on accidental I/O during describe/build/start.
struct NoResources;
impl ResourceAccess for NoResources {
    fn read_file(&self, _: PathBuf, _: usize) -> BoxFuture<'_, Result<Vec<u8>, BuildError>> {
        panic!("undeclared file")
    }
    fn listen_tcp(
        &self,
        _: Destination,
    ) -> BoxFuture<'_, Result<zero_platform_tokio::ControlledTcpListener, BuildError>> {
        panic!("undeclared TCP listener")
    }
    fn listen_datagram(
        &self,
        _: Destination,
    ) -> BoxFuture<
        '_,
        Result<zero_platform_tokio::contract::component::GrantedDatagramListener, BuildError>,
    > {
        panic!("undeclared datagram listener")
    }
    fn open_device(&self, _: Arc<str>) -> BoxFuture<'_, Result<BoxPacketSession, BuildError>> {
        panic!("undeclared device")
    }
}

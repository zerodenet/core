use super::*;
use std::{num::NonZeroUsize, time::Duration};
use tokio::net::TcpListener;
use zero_core::{
    component::{ComponentInstanceId, NetworkGeneration, ResourceGeneration},
    Address,
};
use zero_platform_tokio::{
    contract::{
        component::{ControlledStreamDemandClass, ResourceAccess},
        lifecycle::ResourceLifecycle,
        network::{CarrierContext, UpstreamBinding, UpstreamTransport},
        scope::TaskScope,
    },
    controlled_carrier::{CarrierOwner, NativeCarrierOpener},
};
fn one() -> NonZeroUsize {
    NonZeroUsize::new(1).unwrap()
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}
fn instance() -> ComponentInstanceId {
    ComponentInstanceId {
        component: ComponentId(981),
        generation: ResourceGeneration(4),
    }
}
struct Fixture {
    resources: Arc<HostResources>,
    scope: TaskScope,
    owner: CarrierOwner,
    declaration: ResourceDeclaration,
    listener: TcpListener,
    transport: UpstreamTransport,
    host_cancel: CancellationToken,
}
impl Fixture {
    async fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer = listener.local_addr().unwrap();
        let egress = Arc::new(zero_platform_tokio::EgressInterfaceControl::default());
        let binding = UpstreamBinding {
            prefix: Vec::new(),
            dial: Default::default(),
            network_generation: NetworkGeneration(egress.generation()),
        };
        let declaration = ResourceDeclaration::ControlledOutboundStream {
            endpoint: Destination::new(Address::Ipv4([127, 0, 0, 1]), peer.port()),
            binding: binding.clone(),
            demand: ControlledStreamDemandClass::Service,
            max_sockets: one(),
        };
        let scope = TaskScope::suspended(tokio::runtime::Handle::current(), Some(one()));
        let host_cancel = CancellationToken::new();
        let resources = HostResources::new_for_instance(
            instance(),
            &[declaration.clone()],
            host_cancel.clone(),
            scope.registrar().scope_identity(),
        )
        .unwrap();
        // Distinct parent deliberately verifies the actual provider's frozen
        // cancellation fence rather than accidental sharing with this owner.
        let owner = CarrierOwner::new(instance(), one(), &CancellationToken::new());
        let transport = UpstreamTransport::new(
            binding.clone(),
            Arc::new(NativeCarrierOpener::new(instance(), binding, peer, egress, None).unwrap()),
        );
        Self {
            resources,
            scope,
            owner,
            declaration,
            listener,
            transport,
            host_cancel,
        }
    }
    fn destination(&self) -> Destination {
        match &self.declaration {
            ResourceDeclaration::ControlledOutboundStream { endpoint, .. } => endpoint.clone(),
            _ => unreachable!(),
        }
    }
    fn context(&self) -> CarrierContext {
        CarrierContext {
            session: None,
            deadline: Some(deadline()),
            cancel: CancellationToken::new(),
        }
    }
}
#[path = "resource_carrier/identity.rs"]
mod identity;
#[path = "resource_carrier/native.rs"]
mod native;

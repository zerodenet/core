//! Real Engine + current Slot + registered sole owner; no synthetic host issuer.
use super::super::{
    business_carrier::BusinessCarrierRequest, business_demand::AdmittedTcpDemand,
    catalog::supervision::testing::TestContext, direct_bridge::Invocation, instance::ComponentSlot,
    stream_bridge::network::NodeOpener, ComponentRecipe,
};
use crate::Proxy;
use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::{timeout, Instant},
};
use zero_core::{
    component::*, request::Destination, Address, Network, ProtocolType, Session, SessionAuth,
};
use zero_platform_tokio::{
    contract::{
        component::{
            BuildContext, ComponentFactory, ComponentInstance, ComponentRoles, ComponentSpec,
            ControlledStreamDemandClass, ResourceAccess, ResourceDeclaration, ResourceOwner,
        },
        lifecycle::{erase_lifecycle, BuildError, CloseReport, ResourceLifecycle, TaskClass},
        network::{CarrierContext, UpstreamBinding, UpstreamOpener},
        scope::TaskRegistrar,
        CancellationToken, ChannelError, CloseReason, DeliveryState, Impact, Reason, Stage,
    },
    controlled_carrier::{CarrierGrants, CarrierOwner, CarrierOwnerRegistration},
};
fn one() -> NonZeroUsize {
    NonZeroUsize::new(1).unwrap()
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}
fn proxy() -> Proxy {
    Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap()
}
fn admitted(proxy: &Proxy) -> (Session, Invocation) {
    let mut session = Session::new(
        0,
        Address::Domain("original-business.test".into()),
        443,
        Network::Tcp,
        ProtocolType::new("verified"),
    );
    session.source_ip = Some(Address::Ipv4([192, 0, 2, 8]));
    session.source_port = Some(48000);
    session.inbound_local = Some((Address::Ipv4([192, 0, 2, 1]), 1080));
    session.original_target = Some(Address::Domain("original-business.test".into()));
    session.inbound_peer_identity = Some(Arc::from("actual-peer"));
    session.sni = Some("original-business.test".into());
    let mut auth = SessionAuth::new("verified");
    auth.principal_key = Some("actual-business-carrier".into());
    auth.quota_remaining_bytes = Some(64);
    auth.up_bps = Some(4096);
    auth.down_bps = Some(8192);
    auth.device_limit = Some(1);
    auth.policy_revision = Some(7);
    session.apply_auth(auth);
    let receipt = proxy
        .engine()
        .prepare_session_admitted(&mut session, "in")
        .unwrap();
    let demand = AdmittedTcpDemand::new(proxy.engine(), SessionId(session.id), receipt).unwrap();
    let invocation = proxy
        .tcp_runtime_services()
        .for_inbound("in".into())
        .with_admitted_business_identity(&session, demand)
        .unwrap()
        .execution()
        .invocation
        .unwrap();
    (session, invocation)
}
#[derive(Default)]
struct State {
    grants: Mutex<Option<CarrierGrants>>,
    tasks: Mutex<Option<TaskRegistrar>>,
    registration: Mutex<Option<CarrierOwnerRegistration>>,
}
struct Factory {
    state: Arc<State>,
    declaration: ResourceDeclaration,
}
struct Owner {
    owner: CarrierOwner,
    resources: Arc<dyn ResourceAccess>,
    tasks: TaskRegistrar,
}
impl ComponentFactory for Factory {
    type Options = ();
    fn describe(&self, _: &()) -> ComponentSpec {
        ComponentSpec {
            owner: ResourceOwner::Component,
            roles: vec![],
            dependencies: vec![],
            static_identity: Arc::from([]),
            resources: vec![self.declaration.clone()],
            task_limit: Some(one()),
        }
    }
    fn build(&self, _: Arc<()>, context: BuildContext) -> Result<ComponentInstance, BuildError> {
        let owner = CarrierOwner::new(
            ComponentInstanceId {
                component: context.component,
                generation: context.generation,
            },
            one(),
            &context.cancel,
        );
        *self.state.grants.lock().unwrap() = Some(owner.grants());
        *self.state.tasks.lock().unwrap() = Some(context.tasks.clone());
        *self.state.registration.lock().unwrap() = Some(owner.registration());
        Ok(ComponentInstance {
            roles: ComponentRoles::default(),
            lifecycle: erase_lifecycle(Owner {
                owner,
                resources: context.resources,
                tasks: context.tasks,
            }),
        })
    }
}
impl ResourceLifecycle for Owner {
    async fn start(&mut self, end: Instant) -> Result<(), BuildError> {
        self.resources
            .register_controlled_carrier(0, self.owner.registration(), &self.tasks)?;
        self.owner.start(end).await
    }
    fn revoke(&self, reason: CloseReason) {
        let _ = reason;
        self.owner.revoke();
    }
    async fn shutdown(&mut self, end: Instant) -> Result<CloseReport, BuildError> {
        self.owner.shutdown(end).await
    }
}
struct Fixture {
    proxy: Proxy,
    slot: Arc<ComponentSlot>,
    state: Arc<State>,
    listener: TcpListener,
    endpoint: Destination,
    binding: UpstreamBinding,
}
impl Fixture {
    async fn new() -> Self {
        let proxy = proxy();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = Destination::new(
            Address::Ipv4([127, 0, 0, 1]),
            listener.local_addr().unwrap().port(),
        );
        let binding = UpstreamBinding {
            prefix: vec![],
            dial: Default::default(),
            network_generation: NetworkGeneration(
                proxy
                    .tcp_runtime_services()
                    .execution()
                    .upstream()
                    .network_generation(),
            ),
        };
        let state = Arc::new(State::default());
        let declaration = ResourceDeclaration::ControlledOutboundStream {
            endpoint: endpoint.clone(),
            binding: binding.clone(),
            demand: ControlledStreamDemandClass::Business,
            max_sockets: one(),
        };
        let recipe = ComponentRecipe::from_validated(
            Arc::new(Factory {
                state: state.clone(),
                declaration,
            }),
            Arc::new(()),
        );
        let slot = Arc::new(ComponentSlot::from_recipe(
            ComponentId(985),
            ResourceGeneration(1),
            recipe,
            Default::default(),
            "actual-business",
        ));
        Self {
            proxy,
            slot,
            state,
            listener,
            endpoint,
            binding,
        }
    }
    fn opener(&self, invocation: &Invocation) -> Result<NodeOpener, ChannelError> {
        let demand = BusinessCarrierRequest::new(self.proxy.engine(), &self.slot, invocation)?;
        Ok(NodeOpener::new(
            self.proxy.tcp_runtime_services().execution().upstream(),
            self.endpoint.clone(),
        )
        .with_business(demand))
    }
    fn grants(&self) -> CarrierGrants {
        self.state.grants.lock().unwrap().as_ref().unwrap().clone()
    }
    fn tasks(&self) -> TaskRegistrar {
        self.state.tasks.lock().unwrap().as_ref().unwrap().clone()
    }
    fn registration(&self) -> CarrierOwnerRegistration {
        self.state
            .registration
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .clone()
    }
}
mod rejection;
mod wire;

pub(in crate::component) mod caller;

pub(in crate::component) mod accepted;
pub(in crate::component) mod sources;

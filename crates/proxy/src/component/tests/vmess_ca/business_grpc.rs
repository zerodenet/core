//! Business library path through the real current host; user closure remains unaccounted.
use super::fixtures::*;
use crate::{
    component::{
        business_carrier::BusinessCarrierRequest, business_demand::AdmittedTcpDemand,
        catalog::supervision::testing::TestContext, instance::ComponentSlot,
        stream_bridge::network::NodeOpener, ComponentRecipe,
    },
    Proxy,
};
use std::{
    num::NonZeroUsize,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::{timeout, Duration},
};
use zero_core::{
    component::*, request::Destination, Address, Network, ProtocolType, Session, SessionAuth,
};
use zero_platform_tokio::{
    contract::{
        component::{
            BuildContext, ComponentFactory, ComponentInstance, ComponentSpec,
            ControlledStreamDemandClass, ResourceAccess, ResourceDeclaration,
        },
        context::{ConnectFact, OpenContext},
        lifecycle::{BuildError, CloseReport},
        network::{UpstreamBinding, UpstreamTransport},
        packet::BoxPacketSession,
        scope::TaskRegistrar,
        stream::{ConnectRequest, PeerConfirmation},
        BoxFuture,
    },
    controlled_carrier::{CarrierOwnerBinding, CarrierOwnerRegistration},
};
use zero_transport::profile::{OwnedClientTlsProfile, OwnedGrpcProfile, OwnedWebSocketProfile};
fn one() -> NonZeroUsize {
    NonZeroUsize::new(1).unwrap()
}
fn proxy() -> Proxy {
    Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap()
}
fn input<'a>(
    port: u16,
    tls: &'a OwnedClientTlsProfile,
    grpc: &'a OwnedGrpcProfile,
) -> vmess::transport::VmessOutboundBuildOptionsRef<
    'a,
    OwnedClientTlsProfile,
    OwnedWebSocketProfile,
    OwnedGrpcProfile,
> {
    vmess::transport::VmessOutboundBuildOptionsRef {
        tag: "actual-business-grpc",
        server: "127.0.0.1",
        port,
        protocol: vmess::transport::VmessOutboundOptionsRef {
            id: ID,
            cipher: "aes-128-gcm",
            mux_concurrency: None,
            mux_idle_timeout_secs: None,
            mux_response_backlog_frames: None,
            mux_response_backlog_bytes: None,
        },
        tls: Some(tls),
        ws: None,
        grpc: Some(grpc),
    }
}
#[derive(Default)]
struct Captured {
    reads: AtomicUsize,
    owner: Mutex<Option<CarrierOwnerRegistration>>,
}
struct Factory(Arc<Captured>);
impl ComponentFactory for Factory {
    type Options = vmess::component::grpc::Options;
    fn describe(&self, options: &Self::Options) -> ComponentSpec {
        vmess::component::grpc::Factory.describe(options)
    }
    fn build(
        &self,
        options: Arc<Self::Options>,
        mut context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        context.resources = Arc::new(Access {
            original: context.resources,
            captured: self.0.clone(),
        });
        vmess::component::grpc::Factory.build(options, context)
    }
}
struct Access {
    original: Arc<dyn ResourceAccess>,
    captured: Arc<Captured>,
}
impl ResourceAccess for Access {
    fn register_controlled_carrier(
        &self,
        ordinal: usize,
        actual: CarrierOwnerRegistration,
        tasks: &TaskRegistrar,
    ) -> Result<CarrierOwnerBinding, BuildError> {
        let binding = self
            .original
            .register_controlled_carrier(ordinal, actual.clone(), tasks)?;
        *self.captured.owner.lock().unwrap() = Some(actual);
        Ok(binding)
    }
    fn read_file(&self, path: PathBuf, max: usize) -> BoxFuture<'_, Result<Vec<u8>, BuildError>> {
        self.captured.reads.fetch_add(1, Ordering::SeqCst);
        self.original.read_file(path, max)
    }
    fn listen_tcp(
        &self,
        destination: Destination,
    ) -> BoxFuture<'_, Result<zero_platform_tokio::ControlledTcpListener, BuildError>> {
        self.original.listen_tcp(destination)
    }
    fn listen_datagram(
        &self,
        destination: Destination,
    ) -> BoxFuture<
        '_,
        Result<zero_platform_tokio::contract::component::GrantedDatagramListener, BuildError>,
    > {
        self.original.listen_datagram(destination)
    }
    fn open_device(&self, name: Arc<str>) -> BoxFuture<'_, Result<BoxPacketSession, BuildError>> {
        self.original.open_device(name)
    }
}
fn admitted(proxy: &Proxy) -> (Session, crate::component::direct_bridge::Invocation) {
    let mut session = Session::new(
        0,
        Address::Domain("native.example".into()),
        443,
        Network::Tcp,
        ProtocolType::new("verified"),
    );
    session.source_ip = Some(Address::Ipv4([192, 0, 2, 12]));
    session.source_port = Some(48012);
    let mut auth = SessionAuth::new("verified");
    auth.principal_key = Some("business-grpc".into());
    auth.quota_remaining_bytes = Some(4096);
    auth.device_limit = Some(1);
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
mod options;
mod wire;

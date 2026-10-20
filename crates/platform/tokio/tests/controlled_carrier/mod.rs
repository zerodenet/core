use super::*;
use crate::contract::{
    lifecycle::{ResourceLifecycle, TaskClass},
    network::{CarrierContext, UpstreamBinding, UpstreamTransport},
    scope::TaskScope,
    CancellationToken,
};
use std::{num::NonZeroUsize, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{timeout, Instant},
};
use zero_core::{
    component::{ComponentId, ComponentInstanceId, NetworkGeneration, ResourceGeneration},
    request::Destination,
    Address,
};
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}
fn instance() -> ComponentInstanceId {
    ComponentInstanceId {
        component: ComponentId(771),
        generation: ResourceGeneration(19),
    }
}
fn destination(address: std::net::SocketAddr) -> Destination {
    Destination::new(
        match address.ip() {
            std::net::IpAddr::V4(ip) => Address::Ipv4(ip.octets()),
            std::net::IpAddr::V6(ip) => Address::Ipv6(ip.octets()),
        },
        address.port(),
    )
}
fn context() -> CarrierContext {
    CarrierContext {
        session: None,
        deadline: Some(deadline()),
        cancel: CancellationToken::new(),
    }
}
struct Fixture {
    owner: CarrierOwner,
    grants: CarrierGrants,
    transport: UpstreamTransport,
    listener: TcpListener,
    destination: Destination,
    egress: Arc<crate::EgressInterfaceControl>,
}
impl Fixture {
    async fn new(cap: usize) -> Self {
        let mut fixture = Self::cold(cap).await;
        fixture.owner.start(deadline()).await.unwrap();
        fixture
    }
    async fn cold(cap: usize) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer = listener.local_addr().unwrap();
        let egress = Arc::new(crate::EgressInterfaceControl::default());
        let binding = UpstreamBinding {
            prefix: vec![],
            dial: zero_traits::DialPolicy {
                source_ip: Some("127.0.0.1".parse().unwrap()),
                ..Default::default()
            },
            network_generation: NetworkGeneration(egress.generation()),
        };
        let opener = Arc::new(
            NativeCarrierOpener::new(instance(), binding.clone(), peer, egress.clone(), None)
                .unwrap(),
        );
        let owner = CarrierOwner::new(
            instance(),
            NonZeroUsize::new(cap).unwrap(),
            &CancellationToken::new(),
        );
        let grants = owner.grants();
        Self {
            owner,
            grants,
            transport: UpstreamTransport::new(binding, opener),
            listener,
            destination: destination(peer),
            egress,
        }
    }
    fn prepare(&self) -> PreparedCarrier {
        self.transport
            .prepare_controlled_stream(
                self.destination.clone(),
                context(),
                self.grants.reserve().unwrap(),
            )
            .unwrap()
    }
    async fn connected(&self) -> (ControlledCarrier, TcpStream) {
        let io = self.prepare().open(deadline()).await.unwrap();
        let (peer, actual_source) = self.listener.accept().await.unwrap();
        assert_eq!(
            actual_source.ip(),
            "127.0.0.1".parse::<std::net::IpAddr>().unwrap()
        );
        (io, peer)
    }
}
#[path = "admission.rs"]
mod admission;
#[path = "early.rs"]
mod early;
#[path = "lifecycle.rs"]
mod lifecycle;
#[path = "opening.rs"]
mod opening;
#[path = "origin.rs"]
mod origin;

mod registration;

mod binding;

mod endpoints;

mod users;

mod auxiliary;

mod reap;

mod request;

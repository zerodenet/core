use super::*;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use std::{error::Error, future::Future, net::SocketAddr, pin::Pin, task::Poll, time::Duration};
use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
    time::{timeout, Instant},
};
use zero_core::{
    component::{ComponentId, ComponentInstanceId, NetworkGeneration, ResourceGeneration},
    request::Destination,
    Address,
};
use zero_platform_tokio::{
    contract::{
        lifecycle::ResourceLifecycle,
        network::{CarrierContext, UpstreamBinding, UpstreamTransport},
        scope::TaskScope,
        Reason,
    },
    controlled_carrier::{CarrierOwner, ControlledCarrier, NativeCarrierOpener},
    EgressInterfaceControl,
};
fn end() -> Instant {
    Instant::now() + Duration::from_secs(5)
}
fn instance() -> ComponentInstanceId {
    ComponentInstanceId {
        component: ComponentId(831),
        generation: ResourceGeneration(7),
    }
}
fn destination(peer: SocketAddr) -> Destination {
    Destination::new(
        match peer.ip() {
            std::net::IpAddr::V4(ip) => Address::Ipv4(ip.octets()),
            std::net::IpAddr::V6(ip) => Address::Ipv6(ip.octets()),
        },
        peer.port(),
    )
}
struct Fixture {
    owner: CarrierOwner,
    transport: UpstreamTransport,
    listener: TcpListener,
    peer: SocketAddr,
}
impl Fixture {
    async fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer = listener.local_addr().unwrap();
        let egress = Arc::new(EgressInterfaceControl::default());
        let binding = UpstreamBinding {
            prefix: vec![],
            dial: Default::default(),
            network_generation: NetworkGeneration(egress.generation()),
        };
        let opener =
            NativeCarrierOpener::new(instance(), binding.clone(), peer, egress, None).unwrap();
        let mut owner = CarrierOwner::new(
            instance(),
            NonZeroUsize::new(1).unwrap(),
            &CancellationToken::new(),
        );
        owner.start(end()).await.unwrap();
        Self {
            owner,
            transport: UpstreamTransport::new(binding, Arc::new(opener)),
            listener,
            peer,
        }
    }
    async fn connected(&self) -> (ControlledCarrier, TcpStream) {
        let carrier = self
            .transport
            .prepare_controlled_stream(
                destination(self.peer),
                CarrierContext {
                    session: None,
                    deadline: Some(end()),
                    cancel: CancellationToken::new(),
                },
                self.owner.grants().reserve().unwrap(),
            )
            .unwrap()
            .open(end())
            .await
            .unwrap();
        let (peer, _) = self.listener.accept().await.unwrap();
        (carrier, peer)
    }
}
fn options(limit: usize, cancel: CancellationToken) -> Options {
    Options {
        delegate_limit: NonZeroUsize::new(limit).unwrap(),
        settings: None,
        keepalive: -1,
        cancel,
    }
}
fn request(data: &'static [u8]) -> http::Request<Full<Bytes>> {
    http::Request::builder()
        .method("POST")
        .uri("https://fixture.test/original")
        .header("host", "fixture.test")
        .body(Full::new(Bytes::from_static(data)))
        .unwrap()
}
async fn pending_once<F: Future>(future: Pin<&mut F>) {
    let mut future = future;
    std::future::poll_fn(|cx| {
        assert!(
            future.as_mut().poll(cx).is_pending(),
            "operation is actually pending before cancellation/rejection"
        );
        Poll::Ready(())
    })
    .await;
}
mod body;
mod outer;
mod rejection;
mod sources;
mod wire;

mod hosted;

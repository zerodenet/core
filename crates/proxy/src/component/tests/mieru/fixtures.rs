mod server;
use crate::Proxy;
pub(super) use server::Server;
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use zero_config::RuntimeConfig;
use zero_core::{
    request::{Destination, ServiceOrigin, ServicePurpose, ServiceRequestFacts},
    Address,
};
use zero_platform_tokio::{
    contract::{
        context::{OpenContext, RequestIdentity},
        network::{CarrierContext, UpstreamBinding, UpstreamOpener, UpstreamTransport},
        stream::{BoxStream, ConnectRequest},
        BoxFuture, CancellationToken, ChannelError,
    },
    controlled_carrier::loan::StreamLoan,
};

pub(super) fn deadline() -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_secs(5)
}
pub(super) fn destination() -> Destination {
    Destination::new(Address::Ipv4([203, 0, 113, 1]), 443)
}
pub(super) fn config(addresses: [SocketAddr; 3]) -> RuntimeConfig {
    RuntimeConfig::parse(&format!(r#"{{"outbounds":[
    {{"tag":"a","protocol":{{"type":"mieru","server":"127.0.0.1","port":{},"username":"u","password":"p"}}}},
    {{"tag":"b","protocol":{{"type":"mieru","server":"127.0.0.1","port":{},"username":"u","password":"p"}}}},
    {{"tag":"c","protocol":{{"type":"mieru","server":"127.0.0.1","port":{},"username":"u","password":"p"}}}},
    {{"tag":"unused","protocol":{{"type":"mieru","server":"127.0.0.1","port":{},"username":"unused","password":"unused"}}}}
    ],"outbound_groups":[{{"tag":"chain","type":"relay","proxies":["a","b","c"]}}],"route":{{"final":{{"type":"direct"}}}}}}"#,addresses[0].port(),addresses[1].port(),addresses[2].port(),addresses[2].port())).unwrap()
}
pub(super) async fn service(proxy: &Proxy) -> zero_platform_tokio::TcpRelayStream {
    try_service(proxy).await.unwrap()
}
pub(super) async fn try_service(
    proxy: &Proxy,
) -> Result<zero_platform_tokio::TcpRelayStream, ChannelError> {
    let services = proxy.tcp_runtime_services();
    let id = services.snapshot().plan().target_id("chain").unwrap();
    let (resolved, _) = services
        .engine()
        .resolve_target_id_in_snapshot(services.snapshot(), id)
        .unwrap();
    let plan =
        crate::runtime::tcp_dispatch::service::prepare(services.protocols(), &services, &resolved)
            .unwrap_or_else(|error| panic!("service preparation failed: {}", error.error))
            .unwrap();
    crate::runtime::tcp_dispatch::service::connect(
        services.execution(),
        destination(),
        plan,
        ServicePurpose::DiagnosticProbe,
        CarrierContext {
            session: None,
            deadline: Some(deadline()),
            cancel: CancellationToken::new(),
        },
    )
    .await
    .map(|connected| connected.stream)
}
struct Denied;
impl UpstreamOpener for Denied {
    fn open_datagram(
        &self,
        _: &UpstreamBinding,
        _: zero_platform_tokio::contract::datagram::DatagramRequest,
        _: CarrierContext,
    ) -> BoxFuture<
        '_,
        Result<zero_platform_tokio::contract::datagram::BoxDatagramSession, ChannelError>,
    > {
        Box::pin(async { panic!("native pool must not retain the caller datagram opener") })
    }

    fn open_stream(
        &self,
        _: &UpstreamBinding,
        _: Destination,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        Box::pin(async {
            panic!("native shared pool must use its actual host provider, never the caller opener")
        })
    }
}
pub(super) async fn loan(proxy: &Proxy) -> StreamLoan {
    try_loan(proxy).await.unwrap()
}
pub(super) async fn try_loan(proxy: &Proxy) -> Result<StreamLoan, ChannelError> {
    let (c, context) = role_context(proxy);
    c.stream(deadline())
        .await
        .unwrap()
        .connect_carrier(
            ConnectRequest {
                destination: destination(),
            },
            context,
        )
        .await
        .map(|connected| connected.io)
}
pub(super) async fn packet(
    proxy: &Proxy,
) -> zero_platform_tokio::contract::datagram::BoxDatagramSession {
    let (c, context) = role_context(proxy);
    c.datagram(deadline())
        .await
        .unwrap()
        .open(
            zero_platform_tokio::contract::datagram::DatagramRequest {
                association: zero_core::component::AssociationId(200),
                targets: zero_core::request::DatagramTargets::Fixed(destination()),
            },
            context,
        )
        .await
        .unwrap()
}
fn role_context(proxy: &Proxy) -> (Arc<crate::component::instance::ComponentSlot>, OpenContext) {
    let services = proxy.tcp_runtime_services();
    let components = services.components.as_ref().unwrap();
    let a = components.direct(Some("a")).unwrap();
    let b = components.direct(Some("b")).unwrap();
    let c = components.direct(Some("c")).unwrap();
    let binding = UpstreamBinding {
        prefix: vec![a.role_id(), b.role_id()],
        dial: c.dial.clone(),
        network_generation: zero_core::component::NetworkGeneration(
            services.upstream().network_generation(),
        ),
    };
    (
        c,
        OpenContext {
            identity: RequestIdentity::Service(Arc::new(ServiceRequestFacts {
                origin: ServiceOrigin::Runtime,
                purpose: ServicePurpose::BackgroundControl,
            })),
            deadline: Some(deadline()),
            cancel: CancellationToken::new(),
            upstream: UpstreamTransport::new(binding, Arc::new(Denied)),
            observer: None,
        },
    )
}

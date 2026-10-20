use super::*;
use zero_platform_tokio::contract::lifecycle::{CloseReport, ResourceLifecycle};
pub(super) struct NativeOwner {
    pub(super) socket: Option<zero_platform_tokio::TokioDatagramSocket>,
    pub(super) address: Option<tokio::sync::oneshot::Sender<std::net::SocketAddr>>,
    pub(super) gate: tokio::net::TcpStream,
    pub(super) pending: Arc<tokio::sync::Notify>,
    pub(super) closed: Arc<AtomicBool>,
}
impl ResourceLifecycle for NativeOwner {
    async fn start(&mut self, _: Instant) -> Result<(), ChannelError> {
        let socket = zero_platform_tokio::TokioDatagramSocket::bind("127.0.0.1:0")
            .await
            .map_err(error)?;
        let address = socket.local_addr().map_err(error)?;
        self.socket = Some(socket);
        self.address.take().unwrap().send(address).unwrap();
        Ok(())
    }
    fn revoke(&self, _: zero_platform_tokio::contract::CloseReason) {}
    async fn shutdown(&mut self, _: Instant) -> Result<CloseReport, ChannelError> {
        if self.socket.is_none() {
            return Ok(CloseReport::default());
        }
        let read = self.gate.read_u8();
        tokio::pin!(read);
        std::future::poll_fn(|cx| {
            let result = read.as_mut().poll(cx);
            if result.is_pending() {
                self.pending.notify_one();
            }
            result
        })
        .await
        .map_err(error)?;
        self.socket.take().unwrap().close().map_err(error)?;
        self.closed.store(true, Ordering::Release);
        Ok(CloseReport {
            closed_io: vec!["original native peer resource explicitly closed".into()],
            ..CloseReport::default()
        })
    }
}
fn error(source: std::io::Error) -> ChannelError {
    ChannelError::new(
        zero_platform_tokio::contract::Stage::Close,
        zero_platform_tokio::contract::Reason::Io,
        zero_platform_tokio::contract::Impact::Component,
    )
    .with_source(source)
}
pub(super) async fn round_trip(
    client: &UdpSocket,
    listener: std::net::SocketAddr,
    echo: &UdpSocket,
    payload: &[u8],
) {
    client.send_to(payload, listener).await.unwrap();
    let mut bytes = [0; 128];
    let (size, peer) = timeout(Duration::from_secs(2), echo.recv_from(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&bytes[..size], payload);
    echo.send_to(&bytes[..size], peer).await.unwrap();
    let (size, _) = timeout(Duration::from_secs(2), client.recv_from(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&bytes[..size], payload);
}
pub(super) async fn initial_listener(
    proxy: &Proxy,
    state: &mut OrchestrationState,
    target: u16,
) -> std::net::SocketAddr {
    let mut config = zero_config::RuntimeConfig::parse(&serde_json::json!({
        "inbounds":[{"tag":"recover-direct","listen":{"address":"127.0.0.1","port":0},"udp":{"enabled":true},
            "protocol":{"type":"direct","target":"127.0.0.1","port":target}}],
        "route":{"final":{"type":"direct"}}
    }).to_string()).unwrap();
    let bound = crate::runtime::listeners::bind_inbound_listener(
        &proxy.protocols,
        &state.inbound_runtime_factory,
        None,
        &config.inbounds[0],
        tokio::sync::watch::channel(false).0,
    )
    .await
    .unwrap();
    let crate::runtime::listeners::PendingInbound::Legacy(
        crate::protocol_registry::BoundInbound::TcpAndDatagram(tcp, _),
    ) = &bound
    else {
        panic!("actual combined listener");
    };
    let address = tcp.local_addr().unwrap();
    config.inbounds[0].listen.port = address.port();
    proxy.engine.stage_runtime_config(config).unwrap();
    let snapshot = proxy.engine.runtime_snapshot();
    proxy
        .protocols
        .start_components(
            snapshot.config().clone(),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
    proxy
        .protocols
        .commit_components(snapshot.config(), || Ok(()))
        .unwrap();
    let factory = crate::runtime::route_runtime::InboundListenerRuntimeFactory::new(
        crate::runtime::route_runtime::SharedIngressRuntimeServices::new(
            proxy.tcp_runtime_services(),
        ),
        proxy.inbound_owners.clone(),
    );
    let (stop, _) = tokio::sync::watch::channel(false);
    let inbound = snapshot.config().inbounds[0].clone();
    crate::runtime::listeners::spawn_inbound_listener(
        &proxy.protocols,
        None,
        &factory,
        &inbound,
        bound,
        stop.clone(),
        &mut state.listeners,
    )
    .unwrap();
    state.listener_stops.insert(inbound.tag.clone(), stop);
    state.active_inbounds.insert(inbound.tag.clone(), inbound);
    state.inbound_runtime_factory = factory;
    state.applied_snapshot = snapshot;
    address
}

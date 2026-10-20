use super::*;
use crate::{
    protocol_registry::PreparedInboundBind,
    runtime::{
        inbound_operation::PreparedInboundListenerOperation,
        inbound_owners::{InboundInstallContext, InboundOwner},
        route_runtime::{InboundListenerRuntime, SharedIngressRuntimeServices},
        Proxy,
    },
};
use std::{
    num::NonZeroUsize,
    pin::Pin,
    sync::atomic::{AtomicBool, Ordering},
};
use zero_platform_tokio::contract::{
    lifecycle::{erase_lifecycle, CloseReport, ResourceLifecycle},
    ChannelError, CloseReason,
};

pub(super) struct Gate {
    pub(super) release: TcpStream,
    pub(super) pending: Arc<tokio::sync::Notify>,
    pub(super) closed: Arc<AtomicBool>,
    pub(super) address: std::net::SocketAddr,
}
struct NativeCleanup {
    socket: Option<zero_platform_tokio::TokioDatagramSocket>,
    gate: TcpStream,
    pending: Arc<tokio::sync::Notify>,
    closed: Arc<AtomicBool>,
}
impl ResourceLifecycle for NativeCleanup {
    async fn start(&mut self, _: Instant) -> Result<(), ChannelError> {
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {}
    async fn shutdown(&mut self, _: Instant) -> Result<CloseReport, ChannelError> {
        if self.socket.is_none() {
            return Ok(CloseReport::default());
        }
        let read = self.gate.read_u8();
        tokio::pin!(read);
        std::future::poll_fn(|cx| {
            let polled = read.as_mut().poll(cx);
            if polled.is_pending() {
                self.pending.notify_one();
            }
            polled
        })
        .await
        .map_err(source)?;
        self.socket.take().unwrap().close().map_err(source)?;
        self.closed.store(true, Ordering::Release);
        Ok(CloseReport {
            closed_io: vec!["original gated native UDP closed".into()],
            ..Default::default()
        })
    }
}
fn source(error: std::io::Error) -> ChannelError {
    use zero_platform_tokio::contract::{Impact, Reason, Stage};
    ChannelError::new(Stage::Close, Reason::Io, Impact::Component).with_source(error)
}
struct WithCleanup {
    operation: Box<dyn PreparedInboundListenerOperation>,
    cleanup: Option<NativeCleanup>,
}
impl PreparedInboundListenerOperation for WithCleanup {
    fn internal_tasks(&self) -> usize {
        self.operation.internal_tasks()
    }
    fn install_resources(&mut self, context: &InboundInstallContext) -> Result<(), EngineError> {
        self.operation.install_resources(context)?;
        if let Some(cleanup) = self.cleanup.take() {
            // Registered after the native listener: shutdown must finish this
            // actual resource before closing the original listener descriptor.
            context.register(erase_lifecycle(cleanup))?;
        }
        Ok(())
    }
    fn execute(
        self: Box<Self>,
        runtime: InboundListenerRuntime,
        bound: crate::protocol_registry::BoundInbound,
        shutdown: watch::Receiver<bool>,
    ) -> Pin<Box<dyn Future<Output = Result<(), EngineError>> + Send + 'static>> {
        self.operation.execute(runtime, bound, shutdown)
    }
}
pub(super) struct Fixture {
    pub(super) proxy: Proxy,
    pub(super) old: Arc<RuntimeConfig>,
    pub(super) rollback: InboundListenerRuntimeFactory,
    pub(super) active: HashMap<String, InboundConfig>,
    pub(super) stops: HashMap<String, watch::Sender<bool>>,
    pub(super) observers: JoinSet<Result<(), EngineError>>,
    pub(super) exits: usize,
    pub(super) gate: Gate,
    pub(super) originals: Vec<Arc<InboundOwner>>,
}
impl Fixture {
    pub(super) async fn new(count: usize) -> Self {
        Self::new_protocol(count, serde_json::json!({"type":"socks5"})).await
    }
    pub(super) async fn new_protocol(count: usize, protocol: serde_json::Value) -> Self {
        let mut ports = Vec::new();
        for _ in 0..count {
            ports.push(TcpListener::bind("127.0.0.1:0").await.unwrap());
        }
        let inbounds: Vec<_> = ports.iter().enumerate().map(|(index, port)| serde_json::json!({
            "tag":format!("old-{index}"), "listen":{"address":"127.0.0.1", "port":port.local_addr().unwrap().port()},
            "protocol":protocol
        })).collect();
        let config = RuntimeConfig::parse(
            &serde_json::json!({"inbounds":inbounds,
            "route":{"final":{"type":"direct"}}})
            .to_string(),
        )
        .unwrap();
        let proxy = Proxy::new(config).unwrap();
        let old = proxy.engine().config();
        proxy
            .protocols
            .start_components(old.clone(), Instant::now() + Duration::from_secs(2))
            .await
            .unwrap();
        proxy.protocols.commit_components(&old, || Ok(())).unwrap();
        let rollback = factory(&proxy);
        let native = zero_platform_tokio::TokioDatagramSocket::bind("127.0.0.1:0")
            .await
            .unwrap();
        let native_address = native.local_addr().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let release = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (gate, _) = listener.accept().await.unwrap();
        let pending = Arc::new(tokio::sync::Notify::new());
        let closed = Arc::new(AtomicBool::new(false));
        let mut cleanup = Some(NativeCleanup {
            socket: Some(native),
            gate,
            pending: pending.clone(),
            closed: closed.clone(),
        });
        let mut stops = HashMap::new();
        let mut active = HashMap::new();
        let mut observers = JoinSet::new();
        let mut originals = Vec::new();
        for (index, port) in ports.into_iter().enumerate() {
            let inbound = &old.inbounds[index];
            let prepared = match proxy
                .protocols
                .prepare_inbound_bind(inbound.clone(), None)
                .unwrap()
            {
                Some(prepared) => prepared,
                None => {
                    // Fixture-only original TCP ownership for a legacy adapter.
                    // This does not opt that adapter into production grants.
                    let operation = proxy
                        .protocols
                        .prepare_inbound_listener(inbound.clone(), None)
                        .unwrap();
                    let declaration =
                        zero_platform_tokio::contract::component::ResourceDeclaration::TcpListener {
                            destination: zero_core::request::Destination::new(
                                zero_core::Address::Ipv4([127, 0, 0, 1]),
                                inbound.listen.port,
                            ),
                            max_bind_attempts: NonZeroUsize::new(50).unwrap(),
                        };
                    PreparedInboundBind::new(operation, Arc::from([declaration])).unwrap()
                }
            };
            let resources = Arc::from(prepared.resources());
            let operation = prepared.into_operation();
            let operation: Box<dyn PreparedInboundListenerOperation> = if index + 1 == count {
                Box::new(WithCleanup {
                    operation,
                    cleanup: cleanup.take(),
                })
            } else {
                operation
            };
            let prepared = PreparedInboundBind::new(operation, resources).unwrap();
            let (stop, _) = watch::channel(false);
            let mut lease = proxy
                .inbound_owners
                .reserve_listener(
                    &inbound.tag,
                    &old,
                    &proxy.protocols,
                    prepared,
                    stop.clone(),
                    NonZeroUsize::new(1).unwrap(),
                )
                .unwrap();
            drop(port);
            lease.bind_tcp().unwrap();
            let bound = PendingInbound::Leased(lease.finish().unwrap());
            let owner = spawn_inbound_listener(
                &proxy.protocols,
                None,
                &rollback,
                inbound,
                bound,
                stop.clone(),
                &mut observers,
            )
            .unwrap();
            owner.wait_ready().await.unwrap();
            originals.push(owner);
            stops.insert(inbound.tag.clone(), stop);
            active.insert(inbound.tag.clone(), inbound.clone());
        }
        Self {
            proxy,
            old,
            rollback,
            active,
            stops,
            observers,
            exits: 0,
            gate: Gate {
                release,
                pending,
                closed,
                address: native_address,
            },
            originals,
        }
    }
    pub(super) fn state(&mut self) -> InboundReconcileState<'_> {
        InboundReconcileState {
            listener_stops: &mut self.stops,
            active_inbounds: &mut self.active,
            expected_listener_exits: &mut self.exits,
            listeners: &mut self.observers,
        }
    }
    pub(super) async fn finish(mut self) {
        for stop in self.stops.values() {
            stop.send_replace(true);
        }
        for tag in self.active.keys() {
            self.proxy
                .inbound_owners
                .shutdown(tag, Instant::now() + Duration::from_secs(2))
                .await
                .unwrap();
        }
        self.proxy
            .inbound_owners
            .retire_bind_reservations_reported(Instant::now() + Duration::from_secs(2), |_, _| {
                Ok(())
            })
            .await
            .unwrap();
        while let Some(result) = self.observers.join_next().await {
            result.unwrap().unwrap();
        }
        self.proxy
            .protocols
            .shutdown_components(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap();
    }
}
pub(super) fn factory(proxy: &Proxy) -> InboundListenerRuntimeFactory {
    InboundListenerRuntimeFactory::new(
        SharedIngressRuntimeServices::new(proxy.tcp_runtime_services()),
        proxy.inbound_owners.clone(),
    )
}
pub(super) async fn ready_socks(port: u16) {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    stream.write_all(&[5, 1, 0]).await.unwrap();
    let mut selected = [0; 2];
    stream.read_exact(&mut selected).await.unwrap();
    assert_eq!(selected, [5, 0]);
}

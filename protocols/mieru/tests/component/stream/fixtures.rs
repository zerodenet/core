use super::*;
use crate::{
    client::ClientPool,
    component::{
        test_support::{context_with, Loopback},
        PoolFactory, PoolOptions,
    },
    transport::MieruOutboundOptionsRef,
};
use std::num::NonZeroUsize;
use zero_core::{
    component::*,
    request::{ServiceOrigin, ServicePurpose, ServiceRequestFacts},
    Address, InboundRouteMultiplexer,
};
use zero_platform_tokio::{
    contract::{
        context::{ConnectObserver, OpenContext, RequestIdentity},
        scope::TaskScope,
        stream::DynStreamOutbound,
        CancellationToken,
    },
    TcpRelayStream,
};

#[derive(Default)]
pub(super) struct Observations {
    pub(super) facts: Mutex<Vec<(Option<SessionId>, ConnectFact)>>,
    pub(super) traffic: Mutex<Vec<(Option<SessionId>, u64, u64)>>,
}
impl ConnectObserver for Observations {
    fn record(&self, session: Option<SessionId>, fact: ConnectFact) {
        self.facts.lock().unwrap().push((session, fact));
    }
    fn protocol_traffic(&self, session: Option<SessionId>, read: u64, written: u64) {
        self.traffic.lock().unwrap().push((session, read, written));
    }
}
pub(super) fn destination() -> Destination {
    Destination::new(Address::Domain("target.example".into()), 443)
}
pub(super) fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}
pub(super) fn prepare(pool: &PoolOptions, address: std::net::SocketAddr) -> PreparedTcp {
    pool.prepare_tcp(TcpPreparation {
        tag: "native-mieru",
        server: "127.0.0.1",
        port: address.port(),
        protocol: MieruOutboundOptionsRef {
            username: "u",
            password: "p",
            udp: false,
            options: &Default::default(),
        },
        dial: Default::default(),
    })
    .unwrap()
}
pub(super) struct Harness {
    pub(super) pool: Arc<ClientPool>,
    pub(super) opener: Arc<Loopback>,
    pub(super) owner: ComponentInstance,
    pub(super) scope: TaskScope,
    pub(super) roles: Vec<ComponentInstance>,
    scopes: Vec<TaskScope>,
    upstream: zero_platform_tokio::contract::network::UpstreamTransport,
}
impl Harness {
    pub(super) async fn new(address: std::net::SocketAddr, count: usize) -> Self {
        let pool = Arc::new(ClientPool::default());
        let opener = Arc::new(Loopback::default());
        let (scope, context) = context_with(1, opener.clone());
        let actual = context.instance();
        let upstream = context.background_carrier.clone().unwrap();
        let options = Arc::new(pool.component_options(NonZeroUsize::new(1).unwrap()));
        let mut owner = PoolFactory.build(options.clone(), context).unwrap();
        let mut roles = Vec::new();
        let mut scopes = Vec::new();
        for index in 0..count {
            let prepared = prepare(&options, address);
            let stream = Arc::new(prepared.bind(actual.component));
            let spec = StreamFactory.describe(&stream);
            assert_eq!(spec.dependencies, vec![actual.component]);
            assert!(spec.resources.is_empty());
            assert_eq!(spec.task_limit, None);
            let (scope, mut context) = context_with(0, opener.clone());
            context.component = ComponentId(62 + index as u64);
            context.dependencies = vec![actual].into();
            context.roles.outbound = Some(OutboundId(100 + index as u64));
            context.background_carrier = None;
            let mut role = StreamFactory.build(stream, context).unwrap();
            assert_eq!(
                role.lifecycle.start(deadline()).await.unwrap_err().reason,
                Reason::Revoked
            );
            roles.push(role);
            scopes.push(scope);
        }
        owner.lifecycle.start(deadline()).await.unwrap();
        for role in &mut roles {
            role.lifecycle.start(deadline()).await.unwrap();
        }
        Self {
            pool,
            opener,
            owner,
            scope,
            roles,
            scopes,
            upstream,
        }
    }
    pub(super) fn role(&self, index: usize) -> Arc<dyn DynStreamOutbound> {
        self.roles[index].roles.stream.as_ref().unwrap().clone()
    }
    pub(super) fn context(&self, observer: Arc<Observations>) -> OpenContext {
        OpenContext {
            identity: RequestIdentity::Service(Arc::new(ServiceRequestFacts {
                origin: ServiceOrigin::Runtime,
                purpose: ServicePurpose::DiagnosticProbe,
            })),
            deadline: Some(deadline()),
            cancel: CancellationToken::new(),
            observer: Some(observer),
            upstream: self.upstream.clone(),
        }
    }
    pub(super) async fn close(&mut self) {
        for role in self.roles.iter_mut().rev() {
            assert!(role
                .lifecycle
                .shutdown(deadline())
                .await
                .unwrap()
                .is_complete());
        }
        let end = deadline();
        let (protocol, tasks) =
            tokio::join!(self.owner.lifecycle.shutdown(end), self.scope.shutdown(end));
        let opened = self.opener.opens.load(std::sync::atomic::Ordering::SeqCst) > 0;
        let report = protocol.unwrap();
        if opened {
            assert!(matches!(report.unfinished.as_slice(), [Unfinished::Io(_)]));
        } else {
            assert!(report.is_complete());
        }
        assert!(report.closed_io.is_empty());
        assert!(report.exited_tasks.is_empty());
        assert!(tasks.is_complete());
        assert_eq!(tasks.exited_tasks.len(), usize::from(opened));
        assert!(tasks.failed_tasks.is_empty());
        for scope in &mut self.scopes {
            assert!(scope.shutdown(deadline()).await.is_complete());
        }
    }
}
pub(super) async fn accept(
    listener: tokio::net::TcpListener,
) -> crate::inbound::multiplex::MieruInboundMultiplexer {
    let (socket, _) = listener.accept().await.unwrap();
    crate::inbound::MieruInboundProfile::from_config(vec![("u".into(), "p".into())])
        .accept_multiplexer(TcpRelayStream::new(socket))
        .await
        .unwrap()
}
pub(super) async fn next_request(
    connection: &crate::inbound::multiplex::MieruInboundMultiplexer,
) -> crate::inbound::multiplex::MieruLogicalStream {
    let mut stream = connection.accept_next().await.unwrap().unwrap();
    let mut expected = vec![5, 1, 0, 3, 14];
    expected.extend_from_slice(b"target.example");
    expected.extend_from_slice(&[1, 187]);
    let mut request = vec![0; expected.len()];
    stream.read_exact(&mut request).await.unwrap();
    assert_eq!(request, expected);
    stream
}

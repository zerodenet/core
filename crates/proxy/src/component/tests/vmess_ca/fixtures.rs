pub(super) use crate::component::instance::HostResources;
use std::{
    net::SocketAddr,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
};
use tokio::time::{Duration, Instant};
use zero_core::{
    component::*,
    request::{Destination, ServiceOrigin, ServicePurpose, ServiceRequestFacts},
    Address,
};
use zero_platform_tokio::contract::{
    component::{BuildContext, ComponentFactory, ComponentInstance, ResourceDeclaration},
    context::{ConnectFact, ConnectObserver, OpenContext, RequestIdentity},
    datagram::{BoxDatagramSession, DatagramRequest},
    network::{CarrierContext, UpstreamBinding, UpstreamOpener, UpstreamTransport},
    scope::TaskScope,
    stream::BoxStream,
    BoxFuture, CancellationToken, ChannelError, Impact, Reason, Stage,
};
use zero_transport::profile::{OwnedClientTlsProfile, OwnedGrpcProfile, OwnedWebSocketProfile};
pub(super) const ID: &str = "11111111-2222-3333-4444-555555555555";
pub(super) const REQUEST_LEN: u64 = 118;
pub(super) fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}
pub(super) fn destination() -> Destination {
    Destination::new(Address::Domain("native.example".into()), 443)
}
pub(super) struct Directory(pub(super) PathBuf);
impl Directory {
    pub(super) fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "zero-vmess-ca-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
#[derive(Default, Debug)]
pub(super) struct Observations {
    pub(super) facts: Mutex<Vec<(Option<SessionId>, ConnectFact)>>,
    pub(super) protocol: Mutex<Option<(u64, u64)>>,
    pub(super) outer: AtomicU64,
    pub(super) dials: AtomicUsize,
}
impl ConnectObserver for Observations {
    fn record(&self, session: Option<SessionId>, fact: ConnectFact) {
        self.facts.lock().unwrap().push((session, fact));
    }
    fn protocol_traffic(&self, _: Option<SessionId>, read: u64, written: u64) {
        *self.protocol.lock().unwrap() = Some((read, written));
    }
}
impl zero_traits::IoObserver for Observations {
    fn stream_boundary(&self) {}
    fn received(&self, _: usize) {}
    fn sent(&self, bytes: usize) {
        self.outer.fetch_add(bytes as u64, Ordering::Relaxed);
    }
    fn error(&self) {}
    fn dropped(&self) {}
}
struct Opener {
    address: SocketAddr,
    observations: Arc<Observations>,
}
impl UpstreamOpener for Opener {
    fn open_stream(
        &self,
        binding: &UpstreamBinding,
        destination: Destination,
        context: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        assert!(binding.prefix.is_empty());
        assert_eq!(context.session, None);
        assert_eq!(destination.port, self.address.port());
        Box::pin(async move {
            self.observations.dials.fetch_add(1, Ordering::Relaxed);
            tokio::net::TcpStream::connect(self.address)
                .await
                .map(|io| {
                    Box::pin(zero_transport::observed::ObservedStream::new(
                        io,
                        self.observations.clone(),
                    )) as BoxStream
                })
                .map_err(|source| {
                    ChannelError::new(Stage::Dial, Reason::Io, Impact::Session).with_source(source)
                })
        })
    }
    fn open_datagram(
        &self,
        _: &UpstreamBinding,
        _: DatagramRequest,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxDatagramSession, ChannelError>> {
        panic!("VMess CA Stream cannot open datagrams")
    }
}
pub(super) fn context(address: SocketAddr, observations: Arc<Observations>) -> OpenContext {
    let network = zero_platform_tokio::EgressInterfaceControl::default();
    OpenContext {
        identity: RequestIdentity::Service(Arc::new(ServiceRequestFacts {
            origin: ServiceOrigin::Runtime,
            purpose: ServicePurpose::DiagnosticProbe,
        })),
        deadline: Some(deadline()),
        cancel: CancellationToken::new(),
        observer: Some(observations.clone()),
        upstream: UpstreamTransport::new(
            UpstreamBinding {
                prefix: vec![],
                dial: Default::default(),
                network_generation: NetworkGeneration(network.generation()),
            },
            Arc::new(Opener {
                address,
                observations,
            }),
        ),
    }
}
pub(super) fn options(
    address: SocketAddr,
    tls: &OwnedClientTlsProfile,
    ws: Option<&OwnedWebSocketProfile>,
    grpc: Option<&OwnedGrpcProfile>,
    base: &Path,
    bound: usize,
) -> vmess::component::Options {
    vmess::component::Options::from_options_refs_at(
        vmess::transport::VmessOutboundBuildOptionsRef {
            tag: "file-ca",
            server: "127.0.0.1",
            port: address.port(),
            protocol: vmess::transport::VmessOutboundOptionsRef {
                id: ID,
                cipher: "aes-128-gcm",
                mux_concurrency: None,
                mux_idle_timeout_secs: None,
                mux_response_backlog_frames: None,
                mux_response_backlog_bytes: None,
            },
            tls: Some(tls),
            ws,
            grpc,
        },
        Default::default(),
        Some(base),
        NonZeroUsize::new(bound),
    )
    .unwrap()
}
pub(super) fn build(
    options: vmess::component::Options,
    expected_path: &Path,
    bound: usize,
) -> (ComponentInstance, TaskScope, Arc<HostResources>) {
    let spec = vmess::component::Factory.describe(&options);
    assert_eq!(
        spec.resources,
        vec![ResourceDeclaration::File {
            path: expected_path.to_path_buf(),
            max_bytes: bound
        }]
    );
    assert_eq!(spec.task_limit, None);
    let cancel = CancellationToken::new();
    let grants = HostResources::new(ComponentId(301), &spec.resources, cancel.clone()).unwrap();
    let scope = TaskScope::empty(tokio::runtime::Handle::current());
    let context = BuildContext {
        component: ComponentId(301),
        generation: ResourceGeneration(2),
        dependencies: Arc::from([]),
        roles: ComponentRoleIds {
            outbound: Some(OutboundId(302)),
            ..Default::default()
        },
        resources: grants.clone(),
        tasks: scope.registrar(),
        cancel,
        background_carrier: None,
    };
    (
        vmess::component::Factory
            .build(Arc::new(options), context)
            .unwrap(),
        scope,
        grants,
    )
}
pub(super) fn tls_profile() -> OwnedClientTlsProfile {
    let mut options = zero_traits::ClientTlsOptions::default();
    options.disable_system_roots = true;
    OwnedClientTlsProfile {
        options,
        server_name: Some("localhost".into()),
        disable_sni: false,
        ca_cert_path: Some("identity.pem".into()),
        insecure: false,
        alpn: vec![],
        client_fingerprint: Some("none".into()),
    }
}
pub(super) fn tls_server(h2: bool) -> (tokio_rustls::TlsAcceptor, String) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(
        vec![cert.cert.der().clone()],
        rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()).into(),
    )
    .unwrap();
    if h2 {
        config.alpn_protocols = vec![b"h2".to_vec()];
    }
    (
        tokio_rustls::TlsAcceptor::from(Arc::new(config)),
        cert.cert.pem(),
    )
}
pub(super) fn inbound() -> vmess::inbound::VmessInboundProfile {
    vmess::inbound::VmessInboundProfile::from_users(vec![vmess::inbound::VmessUser::from_config(
        ID,
        "aes-128-gcm",
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap()])
}
pub(super) fn io_kind(error: &(dyn std::error::Error + 'static)) -> Option<std::io::ErrorKind> {
    if let Some(error) = error.downcast_ref::<std::io::Error>() {
        Some(error.kind())
    } else {
        error.source().and_then(io_kind)
    }
}

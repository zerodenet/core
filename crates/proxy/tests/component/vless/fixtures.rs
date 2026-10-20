use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};
use tokio::net::TcpStream;
use zero_core::{
    component::{NetworkGeneration, SessionId},
    request::{Destination, ServiceOrigin, ServicePurpose, ServiceRequestFacts},
    Address,
};
use zero_platform_tokio::contract::{
    component::{ComponentFactory, ComponentInstance},
    context::{ConnectFact, ConnectObserver, OpenContext, RequestIdentity},
    datagram::{BoxDatagramSession, DatagramRequest},
    network::{CarrierContext, UpstreamBinding, UpstreamOpener, UpstreamTransport},
    scope::TaskScope,
    stream::BoxStream,
    BoxFuture, CancellationToken, ChannelError, Impact, Reason, Stage,
};
pub(super) const ID: &str = "11111111-2222-3333-4444-555555555555";
pub(super) const REQUEST_LEN: u64 = 37;
pub(super) fn request() -> Vec<u8> {
    let mut bytes = vec![0];
    bytes.extend_from_slice(&[
        0x11, 0x11, 0x11, 0x11, 0x22, 0x22, 0x33, 0x33, 0x44, 0x44, 0x55, 0x55, 0x55, 0x55, 0x55,
        0x55,
    ]);
    bytes.extend_from_slice(&[0, 1, 1, 0xbb, 2, 14]);
    bytes.extend_from_slice(b"native.example");
    bytes
}
pub(super) fn destination() -> Destination {
    Destination::new(Address::Domain("native.example".into()), 443)
}
pub(super) fn acceptor_with_alpn(h2: bool) -> tokio_rustls::TlsAcceptor {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let key = rustls::pki_types::PrivateKeyDer::from(rustls::pki_types::PrivatePkcs8KeyDer::from(
        cert.signing_key.serialize_der(),
    ));
    let mut config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert.cert.der().clone()], key)
        .unwrap();
    if h2 {
        config.alpn_protocols = vec![b"h2".to_vec()];
    }
    tokio_rustls::TlsAcceptor::from(Arc::new(config))
}
#[derive(Debug, Default)]
pub(super) struct Observations {
    pub(super) facts: Mutex<Vec<(Option<SessionId>, ConnectFact)>>,
    pub(super) protocol: Mutex<Option<(u64, u64)>>,
    pub(super) outer: AtomicU64,
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
pub(super) struct Opener {
    pub(super) address: SocketAddr,
    pub(super) observations: Arc<Observations>,
}
impl UpstreamOpener for Opener {
    fn open_stream(
        &self,
        binding: &UpstreamBinding,
        destination: Destination,
        context: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        assert!(binding.prefix.is_empty());
        assert_eq!(destination.port, self.address.port());
        assert_eq!(context.session, None);
        Box::pin(async move {
            TcpStream::connect(self.address)
                .await
                .map(|io| {
                    super::pinned::wrap(Box::pin(zero_transport::observed::ObservedStream::new(
                        io,
                        self.observations.clone(),
                    )))
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
        panic!("VLESS Stream does not open datagrams")
    }
}
pub(super) fn context(address: SocketAddr, observations: Arc<Observations>) -> OpenContext {
    let network = zero_platform_tokio::EgressInterfaceControl::default();
    OpenContext {
        identity: RequestIdentity::Service(Arc::new(ServiceRequestFacts {
            origin: ServiceOrigin::Runtime,
            purpose: ServicePurpose::DiagnosticProbe,
        })),
        deadline: Some(super::super::fixtures::deadline()),
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
pub(super) fn instance(
    address: SocketAddr,
    tls: Option<&zero_transport::profile::OwnedClientTlsProfile>,
    ws: Option<&zero_transport::profile::OwnedWebSocketProfile>,
    grpc: Option<&zero_transport::profile::OwnedGrpcProfile>,
) -> (ComponentInstance, TaskScope) {
    instance_with_flow(address, tls, ws, grpc, None, &[])
}
pub(super) fn instance_with_flow(
    address: SocketAddr,
    tls: Option<&zero_transport::profile::OwnedClientTlsProfile>,
    ws: Option<&zero_transport::profile::OwnedWebSocketProfile>,
    grpc: Option<&zero_transport::profile::OwnedGrpcProfile>,
    flow: Option<&str>,
    testseed: &[u32],
) -> (ComponentInstance, TaskScope) {
    let options = vless::component::Options::from_options_refs(
        vless::transport::VlessOutboundBuildOptionsRef {
            final_mask: None,
            mkcp: None,
            hysteria: None,
            download: None,
            tag: "native",
            server: "127.0.0.1",
            port: address.port(),
            protocol: vless::transport::VlessOutboundOptionsRef {
                encryption: None,
                id: ID,
                flow,
                testpre: 0,
                testseed,
                mux_concurrency: None,
                xudp_concurrency: None,
                mux_idle_timeout_secs: None,
                mux_response_backlog_frames: None,
                mux_response_backlog_bytes: None,
                reality: None,
                quic: None,
            },
            tls,
            ws,
            grpc,
            h2: None::<&zero_transport::profile::OwnedH2Profile>,
            http_upgrade: None::<&zero_transport::profile::OwnedHttpUpgradeProfile>,
            split_http: None::<&zero_transport::profile::OwnedSplitHttpProfile>,
        },
        Default::default(),
    )
    .unwrap();
    assert!(!format!("{options:?}").contains(ID));
    let (context, scope) = super::super::fixtures::build_context();
    (
        vless::component::Factory
            .build(Arc::new(options), context)
            .unwrap(),
        scope,
    )
}
pub(super) fn tls_profile() -> zero_transport::profile::OwnedClientTlsProfile {
    zero_transport::profile::OwnedClientTlsProfile {
        options: Default::default(),
        server_name: Some("localhost".into()),
        disable_sni: false,
        ca_cert_path: None,
        insecure: true,
        alpn: vec![],
        client_fingerprint: Some("none".into()),
    }
}

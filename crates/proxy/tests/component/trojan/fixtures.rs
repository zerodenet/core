use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
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
pub(super) const PASSWORD: &str = "native-password";
pub(super) fn request() -> Vec<u8> {
    let mut bytes =
        b"e88960794334c8fb553896495e88d9d8f1bebc5b4d35c24628b0c30f\r\n\x01\x03\x0enative.example"
            .to_vec();
    bytes.extend_from_slice(&443u16.to_be_bytes());
    bytes.extend_from_slice(b"\r\n");
    bytes
}
pub(super) fn destination() -> Destination {
    Destination::new(Address::Domain("native.example".into()), 443)
}
pub(super) fn acceptor() -> tokio_rustls::TlsAcceptor {
    acceptor_with_alpn(false)
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
pub(super) async fn echo_server() -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let acceptor = acceptor();
    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut stream = acceptor.accept(stream).await.unwrap();
        let expected = request();
        let mut header = vec![0; expected.len()];
        stream.read_exact(&mut header).await.unwrap();
        assert_eq!(header, expected);
        let mut payload = Vec::new();
        stream.read_to_end(&mut payload).await.unwrap();
        assert_eq!(payload, b"payload");
        stream.write_all(b"after-half-close").await.unwrap();
        stream.shutdown().await.unwrap();
    });
    (address, task)
}
#[derive(Default)]
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
        panic!("Trojan Stream does not open datagrams")
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
pub(super) fn instance(address: SocketAddr, insecure: bool) -> (ComponentInstance, TaskScope) {
    instance_with_profiles(address, insecure, None, None)
}
pub(super) fn instance_with_profiles(
    address: SocketAddr,
    insecure: bool,
    ws: Option<&zero_transport::profile::OwnedWebSocketProfile>,
    grpc: Option<&zero_transport::profile::OwnedGrpcProfile>,
) -> (ComponentInstance, TaskScope) {
    let options = trojan::component::Options::from_options_refs(
        trojan::transport::TrojanOutboundBuildOptionsRef {
            tag: "native",
            server: "127.0.0.1",
            port: address.port(),
            protocol: trojan::transport::TrojanOutboundOptionsRef {
                password: PASSWORD,
                sni: Some("localhost"),
                insecure,
                client_fingerprint: Some("none"),
                mux_concurrency: None,
                mux_idle_timeout_secs: None,
                mux_response_backlog_frames: None,
                mux_response_backlog_bytes: None,
            },
            ws,
            grpc,
        },
        Default::default(),
    )
    .unwrap();
    assert!(!format!("{options:?}").contains(PASSWORD));
    assert!(trojan::component::Factory
        .describe(&options)
        .resources
        .is_empty());
    let (context, scope) = super::super::fixtures::build_context();
    (
        trojan::component::Factory
            .build(Arc::new(options), context)
            .unwrap(),
        scope,
    )
}

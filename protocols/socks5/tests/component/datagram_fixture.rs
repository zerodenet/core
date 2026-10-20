use super::super::fixtures::{deadline, instance_at, Facts};
use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_core::{
    component::{AssociationId, NetworkGeneration},
    request::{DatagramTargets, Destination, ServiceOrigin, ServicePurpose, ServiceRequestFacts},
    Address,
};
use zero_platform_tokio::{
    contract::{
        context::{OpenContext, RequestIdentity},
        datagram::{BoxDatagramSession, DatagramRequest},
        network::{CarrierContext, UpstreamBinding, UpstreamOpener, UpstreamTransport},
        stream::BoxStream,
        BoxFuture, CancellationToken, ChannelError, Impact, Reason, Stage,
    },
    TokioDatagramSocket, TokioSocket,
};
fn destination(peer: SocketAddr) -> Destination {
    Destination::new(
        Address::Ipv4(match peer.ip() {
            std::net::IpAddr::V4(ip) => ip.octets(),
            _ => panic!("IPv4 fixture"),
        }),
        peer.port(),
    )
}
pub(super) struct Opener {
    pub(super) control: SocketAddr,
    pub(super) raw_local: Mutex<Option<SocketAddr>>,
    pub(super) calls: Mutex<Vec<Destination>>,
}
impl UpstreamOpener for Opener {
    fn open_stream(
        &self,
        binding: &UpstreamBinding,
        endpoint: Destination,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        let allowed = binding.prefix.is_empty() && endpoint == destination(self.control);
        Box::pin(async move {
            if !allowed {
                return Err(ChannelError::new(
                    Stage::Prepare,
                    Reason::Unsupported,
                    Impact::Session,
                ));
            }
            self.calls.lock().unwrap().push(endpoint);
            Ok(Box::pin(
                tokio::net::TcpStream::connect(self.control)
                    .await
                    .map_err(|source| {
                        ChannelError::new(Stage::Dial, Reason::Io, Impact::Session)
                            .with_source(source)
                    })?,
            ) as BoxStream)
        })
    }
    fn open_datagram(
        &self,
        binding: &UpstreamBinding,
        request: DatagramRequest,
        context: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxDatagramSession, ChannelError>> {
        let allowed = binding.prefix.is_empty();
        Box::pin(async move {
            if !allowed {
                return Err(ChannelError::new(
                    Stage::Prepare,
                    Reason::Unsupported,
                    Impact::Session,
                ));
            }
            let DatagramTargets::Fixed(endpoint) = request.targets else {
                panic!("one actual relay");
            };
            let Address::Ipv4(ip) = &endpoint.address else {
                panic!("IPv4 relay fixture");
            };
            let peer = SocketAddr::new((*ip).into(), endpoint.port);
            self.calls.lock().unwrap().push(endpoint);
            let socket = TokioDatagramSocket::bind_for_peer_on(peer, None)
                .await
                .unwrap();
            *self.raw_local.lock().unwrap() = Some(socket.local_addr().unwrap());
            Ok(socket.into_fixed_datagram_session(request.association, peer, context.cancel))
        })
    }
}
fn context(opener: Arc<Opener>) -> OpenContext {
    OpenContext {
        identity: RequestIdentity::Service(Arc::new(ServiceRequestFacts {
            origin: ServiceOrigin::Runtime,
            purpose: ServicePurpose::DiagnosticProbe,
        })),
        deadline: Some(deadline()),
        cancel: CancellationToken::new(),
        observer: Some(Arc::new(Facts::default())),
        upstream: UpstreamTransport::new(
            UpstreamBinding {
                prefix: vec![],
                dial: Default::default(),
                network_generation: NetworkGeneration(1),
            },
            opener,
        ),
    }
}
pub(super) async fn server(
    count: usize,
    seen: Arc<Mutex<Vec<(Address, u16, Vec<u8>)>>>,
    eof: Option<Arc<tokio::sync::Notify>>,
    release: Option<Arc<tokio::sync::Notify>>,
) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let relay = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let relay_endpoint = relay.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut control = TokioSocket::new(stream);
        assert!(matches!(
            socks5::Socks5Inbound
                .accept_command(&mut control)
                .await
                .unwrap(),
            socks5::Socks5Request::UdpAssociate(_)
        ));
        socks5::Socks5Inbound
            .send_success_response_with_bound(
                &mut control,
                &Address::Ipv4([127, 0, 0, 1]),
                relay_endpoint.port(),
            )
            .await
            .unwrap();
        let mut bytes = vec![0; 65535];
        let codec = socks5::udp::Socks5InboundUdpCodec;
        for _ in 0..count {
            let (size, peer) = relay.recv_from(&mut bytes).await.unwrap();
            let packet = codec.decode_request(&bytes[..size]).unwrap();
            seen.lock().unwrap().push((
                packet.target().clone(),
                packet.port(),
                packet.payload().to_vec(),
            ));
            let response = codec
                .encode_response_to_client(packet.target(), packet.port(), packet.payload())
                .unwrap();
            relay.send_to(&response, peer).await.unwrap();
        }
        let mut control = control.into_inner();
        let mut extra = Vec::new();
        control.read_to_end(&mut extra).await.unwrap();
        assert!(extra.is_empty());
        if let Some(eof) = eof {
            eof.notify_one();
        }
        if let Some(release) = release {
            release.notified().await;
        }
        control.shutdown().await.unwrap();
    });
    (endpoint, task)
}
pub(super) async fn open(
    opener: Arc<Opener>,
) -> (
    zero_platform_tokio::contract::component::ComponentInstance,
    zero_platform_tokio::contract::scope::TaskScope,
    BoxDatagramSession,
) {
    let (replacement, scope) = instance_at(destination(opener.control), false).await;
    let channel = replacement
        .roles
        .datagram
        .as_ref()
        .unwrap()
        .open(
            DatagramRequest {
                association: AssociationId(8),
                targets: DatagramTargets::Multiple,
            },
            context(opener),
        )
        .await
        .unwrap();
    (replacement, scope, channel)
}

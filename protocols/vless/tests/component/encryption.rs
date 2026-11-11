//! Real standard-role framing over an exclusive in-memory carrier. Native
//! provider closure is qualified separately by the host's original-I/O tests.
use base64::Engine;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use vless::component::{Factory, Options};
use zero_core::{
    component::{AssociationId, ComponentId, ComponentRoleIds, NetworkGeneration},
    packet::Datagram,
    request::{DatagramTargets, ServiceOrigin, ServicePurpose, ServiceRequestFacts},
};
use zero_core::{request::Destination, Address};
use zero_platform_tokio::contract::{
    component::{BuildContext, ComponentFactory, ResourceAccess, RoleKind},
    context::OpenContext,
    lifecycle::BuildError,
    network::CarrierContext,
    packet::BoxPacketSession,
    stream::{BoxStream, ConnectRequest},
    ChannelError, Reason, Stage,
};
use zero_platform_tokio::contract::{
    context::RequestIdentity,
    datagram::{recv, send, BoxDatagramSession, DatagramRequest},
    network::{UpstreamBinding, UpstreamOpener, UpstreamTransport},
    scope::TaskScope,
    BoxFuture, CancellationToken, CloseReason, Impact,
};
use zero_transport::profile::*;

fn options(profile: &str) -> Options {
    Options::from_options_refs(
        vless::transport::VlessOutboundBuildOptionsRef {
            tag: "encrypted",
            server: "127.0.0.1",
            port: 443,
            protocol: vless::transport::VlessOutboundOptionsRef {
                encryption: Some(profile),
                id: "11111111-2222-3333-4444-555555555555",
                flow: None,
                testpre: 0,
                testseed: &[],
                mux_concurrency: None,
                xudp_concurrency: None,
                mux_idle_timeout_secs: None,
                mux_response_backlog_frames: None,
                mux_response_backlog_bytes: None,
                reality: None,
                quic: None,
            },
            tls: None::<&OwnedClientTlsProfile>,
            ws: None::<&OwnedWebSocketProfile>,
            grpc: None::<&OwnedGrpcProfile>,
            h2: None::<&OwnedH2Profile>,
            http_upgrade: None::<&OwnedHttpUpgradeProfile>,
            split_http: None::<&OwnedSplitHttpProfile>,
            final_mask: None,
            mkcp: None,
            hysteria: None,
            download: None,
        },
        Default::default(),
    )
    .unwrap()
}
fn pair(seed: u8) -> (String, vless::encryption::EncryptionServer) {
    let encode = |bytes: &[u8]| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
    let server = vless::encryption::EncryptionServer::new(
        vless::encryption::config::EncryptionConfig::server(&format!(
            "mlkem768x25519plus.native.600s.100-35-35.{}",
            encode(&[seed; 32])
        ))
        .unwrap()
        .unwrap(),
    )
    .unwrap();
    let profile = format!(
        "mlkem768x25519plus.native.0rtt.100-35-35.{}",
        server
            .public_keys()
            .iter()
            .map(|key| encode(key))
            .collect::<Vec<_>>()
            .join(".")
    );
    (profile, server)
}
// These tests acquire no native file/listener/device grants. The supplied
// duplex carrier is the only I/O; it must be returned before shutdown completes.
struct Denied;
fn denied<T: Send + 'static>() -> BoxFuture<'static, Result<T, BuildError>> {
    Box::pin(async {
        Err(ChannelError::new(
            Stage::Prepare,
            Reason::Unsupported,
            Impact::Component,
        ))
    })
}
impl ResourceAccess for Denied {
    fn read_file(
        &self,
        _: std::path::PathBuf,
        _: usize,
    ) -> BoxFuture<'_, Result<Vec<u8>, BuildError>> {
        denied()
    }
    fn listen_tcp(
        &self,
        _: Destination,
    ) -> BoxFuture<'_, Result<zero_platform_tokio::ControlledTcpListener, BuildError>> {
        denied()
    }
    fn listen_datagram(
        &self,
        _: Destination,
    ) -> BoxFuture<
        '_,
        Result<zero_platform_tokio::contract::component::GrantedDatagramListener, BuildError>,
    > {
        denied()
    }
    fn open_device(&self, _: Arc<str>) -> BoxFuture<'_, Result<BoxPacketSession, BuildError>> {
        denied()
    }
}
struct CarrierOnce(Mutex<Option<BoxStream>>);
impl UpstreamOpener for CarrierOnce {
    fn open_stream(
        &self,
        _: &UpstreamBinding,
        target: Destination,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        assert_eq!(target, Destination::new(Address::Ipv4([127, 0, 0, 1]), 443));
        Box::pin(async {
            Ok(self
                .0
                .lock()
                .unwrap()
                .take()
                .expect("one original per open"))
        })
    }
    fn open_datagram(
        &self,
        _: &UpstreamBinding,
        _: DatagramRequest,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxDatagramSession, ChannelError>> {
        Box::pin(async {
            Err(ChannelError::new(
                Stage::Dial,
                Reason::Unsupported,
                Impact::Component,
            ))
        })
    }
}
fn context(io: tokio::io::DuplexStream) -> OpenContext {
    OpenContext {
        identity: RequestIdentity::Service(
            Arc::new(ServiceRequestFacts {
                origin: ServiceOrigin::Runtime,
                purpose: ServicePurpose::DiagnosticProbe,
            })
            .into(),
        ),
        deadline: Some(tokio::time::Instant::now() + std::time::Duration::from_secs(4)),
        cancel: CancellationToken::new(),
        observer: None,
        upstream: UpstreamTransport::new(
            UpstreamBinding {
                prefix: vec![],
                dial: Default::default(),
                network_generation: NetworkGeneration(1),
            },
            Arc::new(CarrierOnce(Mutex::new(Some(Box::pin(io))))),
        ),
    }
}
#[test]
fn standard_encryption_identity_fences_full_keys_without_adding_native_resources_or_tasks() {
    let (first, _) = pair(7);
    let (second, _) = pair(8);
    let first = Factory.describe(&options(&first));
    let same = Factory.describe(&options(&pair(7).0));
    let changed = Factory.describe(&options(&second));
    assert_eq!(first.static_identity, same.static_identity);
    assert_ne!(first.static_identity, changed.static_identity);
    assert_eq!(first.roles, [RoleKind::Stream, RoleKind::Datagram]);
    assert!(first.task_limit.is_none());
    assert!(first.resources.is_empty());
}
#[tokio::test]
async fn standard_encryption_stream_and_datagram_share_the_recipe_and_preserve_wire_and_view_return(
) {
    tokio::time::timeout(std::time::Duration::from_secs(12), async {
        let (profile, server) = pair(7);
        let cancel = CancellationToken::new();
        let mut scope = TaskScope::empty(tokio::runtime::Handle::current());
        let mut instance = Factory
            .build(
                Arc::new(options(&profile)),
                BuildContext {
                    component: ComponentId(809),
                    generation: zero_core::component::ResourceGeneration(1),
                    dependencies: Arc::from([]),
                    roles: ComponentRoleIds::default(),
                    resources: Arc::new(Denied),
                    tasks: scope.registrar().without_tasks(),
                    cancel,
                    background_carrier: None,
                },
            )
            .unwrap();
        let role = instance.roles.stream.clone().unwrap();
        let datagram = instance.roles.datagram.clone().unwrap();
        instance
            .lifecycle
            .start(tokio::time::Instant::now() + std::time::Duration::from_secs(1))
            .await
            .unwrap();
        let target = Destination::new(Address::Ipv4([1, 1, 1, 1]), 53);
        // The same actual recipe/ticket serves both roles and subsequent opens.
        for command in [1, 2, 1, 2] {
            let (local, peer) = tokio::io::duplex(4096);
            let server = server.clone();
            let serving = tokio::spawn(async move {
                let mut io = server.handshake(peer).await.unwrap();
                let mut expected = vec![0];
                expected.extend_from_slice(
                    &vless::parse_uuid("11111111-2222-3333-4444-555555555555").unwrap(),
                );
                expected.extend_from_slice(&[0, command, 0, 53, 1, 1, 1, 1, 1]);
                let mut header = vec![0; expected.len()];
                io.read_exact(&mut header).await.unwrap();
                assert_eq!(header, expected);
                io.write_all(&[0, 0]).await.unwrap();
                io.flush().await.unwrap();
                let payload = if command == 1 {
                    &b"data"[..]
                } else {
                    &b"\x00\x04data"[..]
                };
                let mut received = vec![0; payload.len()];
                io.read_exact(&mut received).await.unwrap();
                assert_eq!(received, payload);
                io.write_all(&received).await.unwrap();
                io.flush().await.unwrap();
                // Wait for the actual transferred stream to be returned.
                assert_eq!(io.read(&mut [0; 1]).await.unwrap(), 0);
            });
            if command == 1 {
                let mut connected = role
                    .connect(
                        ConnectRequest {
                            destination: target.clone(),
                        },
                        context(local),
                    )
                    .await
                    .unwrap();
                connected.io.write_all(b"data").await.unwrap();
                connected.io.flush().await.unwrap();
                let mut received = [0; 4];
                connected.io.read_exact(&mut received).await.unwrap();
                assert_eq!(&received, b"data");
                connected.io.shutdown().await.unwrap();
                drop(connected);
            } else {
                let mut session = datagram
                    .open(
                        DatagramRequest {
                            association: AssociationId(1),
                            targets: DatagramTargets::Fixed(target.clone()),
                        },
                        context(local),
                    )
                    .await
                    .unwrap();
                let frame = Datagram {
                    association: AssociationId(1),
                    address: target.clone(),
                    payload: b"data".to_vec().into(),
                };
                send(
                    session.tx.as_mut(),
                    session.control.as_mut(),
                    &frame,
                    &session.limits,
                )
                .await
                .unwrap();
                let received = recv(session.rx.as_mut()).await.unwrap().unwrap();
                assert_eq!(received.payload.as_ref(), b"data");
                assert!(!session.control.try_close().unwrap().unfinished.is_empty());
                drop(session.tx);
                drop(session.rx);
                assert!(session
                    .control
                    .wait_closed(tokio::time::Instant::now() + std::time::Duration::from_secs(1))
                    .await
                    .unwrap()
                    .unfinished
                    .is_empty());
            }
            serving.await.unwrap();
        }
        instance.lifecycle.revoke(CloseReason::Shutdown);
        let end = tokio::time::Instant::now() + std::time::Duration::from_secs(1);
        assert!(instance
            .lifecycle
            .shutdown(end)
            .await
            .unwrap()
            .unfinished
            .is_empty());
        assert!(scope.shutdown(end).await.unfinished.is_empty());
    })
    .await
    .unwrap();
}

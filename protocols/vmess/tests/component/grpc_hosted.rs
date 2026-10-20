use super::*;
use std::{
    num::NonZeroUsize,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::timeout,
};
use zero_core::{
    component::*,
    request::{Destination, ServiceOrigin, ServicePurpose, ServiceRequestFacts},
    Address,
};
use zero_platform_tokio::{
    contract::{
        component::{BuildContext, ComponentFactory, ResourceAccess},
        context::{OpenContext, RequestIdentity},
        lifecycle::BuildError,
        network::{UpstreamBinding, UpstreamTransport},
        packet::BoxPacketSession,
        scope::{SpawnError, TaskClass, TaskScope},
        stream::{ConnectRequest, PeerConfirmation},
        BoxFuture, CancellationToken,
    },
    controlled_carrier::NativeCarrierOpener,
    EgressInterfaceControl,
};
use zero_transport::profile::{OwnedClientTlsProfile, OwnedGrpcProfile, OwnedWebSocketProfile};
const ID: &str = "ad14d22b-6340-45f4-834e-f09f948c3f9c";
fn end() -> Instant {
    Instant::now() + Duration::from_secs(5)
}
fn destination() -> Destination {
    Destination::new(Address::Domain("original.example".into()), 443)
}
#[derive(Debug, Default)]
struct FailActualRead(AtomicBool);
impl zero_traits::IoObserver for FailActualRead {
    fn stream_boundary(&self) {}
    fn sent(&self, _: usize) {}
    fn received(&self, _: usize) {
        assert!(
            !self.0.load(Ordering::SeqCst),
            "original VMess carrier read failed"
        );
    }
    fn error(&self) {}
    fn dropped(&self) {}
}
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
async fn actual_failure(user_receives: bool) {
    timeout(Duration::from_secs(8), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer = listener.local_addr().unwrap();
        let grpc = OwnedGrpcProfile {
            service_names: vec!["actual-vmess".into()],
            multi_mode: true,
            ..Default::default()
        };
        let server_profile = grpc.clone();
        let (finish, finished) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (io, _) = listener.accept().await.unwrap();
            let incoming =
                zero_transport::grpc::accept_grpc_connection(io, &server_profile).unwrap();
            let carrier = incoming.accept().await.unwrap();
            let profile = crate::inbound::VmessInboundProfile::from_users(vec![
                crate::inbound::VmessUser::from_config(
                    ID,
                    "aes-128-gcm",
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                )
                .unwrap(),
            ]);
            let (facts, mut io) = profile
                .accept_request_stream(
                    crate::inbound::VmessInbound,
                    zero_platform_tokio::TcpRelayStream::new(carrier),
                )
                .await
                .unwrap();
            assert_eq!(facts.destination, destination());
            for expected in [b"live", b"fail"] {
                let mut bytes = [0; 4];
                io.read_exact(&mut bytes).await.unwrap();
                assert_eq!(&bytes, expected);
                io.write_all(&bytes).await.unwrap();
                io.flush().await.unwrap();
            }
            finished.await.unwrap();
            incoming.close();
        });
        let runtime = crate::transport::VmessTransportRuntime::default();
        let cap = NonZeroUsize::new(1).unwrap();
        let options = Options::from_options_refs_at(
            crate::transport::VmessOutboundBuildOptionsRef::<
                OwnedClientTlsProfile,
                OwnedWebSocketProfile,
                OwnedGrpcProfile,
            > {
                tag: "actual",
                server: "127.0.0.1",
                port: peer.port(),
                protocol: crate::transport::VmessOutboundOptionsRef {
                    id: ID,
                    cipher: "aes-128-gcm",
                    mux_concurrency: None,
                    mux_idle_timeout_secs: None,
                    mux_response_backlog_frames: None,
                    mux_response_backlog_bytes: None,
                },
                tls: None,
                ws: None,
                grpc: Some(&grpc),
            },
            Default::default(),
            None,
            None,
            cap,
            &runtime,
        )
        .unwrap();
        let actual = ComponentInstanceId {
            component: ComponentId(912),
            generation: ResourceGeneration(4),
        };
        let egress = Arc::new(EgressInterfaceControl::default());
        let binding = UpstreamBinding {
            prefix: vec![],
            dial: Default::default(),
            network_generation: NetworkGeneration(egress.generation()),
        };
        let observer = Arc::new(FailActualRead::default());
        let upstream = UpstreamTransport::new(
            binding.clone(),
            Arc::new(
                NativeCarrierOpener::new(actual, binding, peer, egress, Some(observer.clone()))
                    .unwrap(),
            ),
        );
        let mut scope = TaskScope::suspended_hosted(tokio::runtime::Handle::current(), Some(cap));
        scope.enable_registration().unwrap();
        let mut owner = Factory
            .build(
                Arc::new(options),
                BuildContext {
                    component: actual.component,
                    generation: actual.generation,
                    dependencies: Arc::from([]),
                    roles: ComponentRoleIds {
                        outbound: Some(OutboundId(913)),
                        ..Default::default()
                    },
                    resources: Arc::new(Denied),
                    tasks: scope.registrar(),
                    cancel: CancellationToken::new(),
                    background_carrier: Some(upstream.clone()),
                },
            )
            .unwrap();
        owner.lifecycle.start(end()).await.unwrap();
        let mut connected = owner
            .roles
            .stream
            .as_ref()
            .unwrap()
            .connect(
                ConnectRequest {
                    destination: destination(),
                },
                OpenContext {
                    identity: RequestIdentity::Service(Arc::new(ServiceRequestFacts {
                        origin: ServiceOrigin::Runtime,
                        purpose: ServicePurpose::DiagnosticProbe,
                    })),
                    deadline: Some(end()),
                    cancel: CancellationToken::new(),
                    upstream,
                    observer: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(connected.peer_confirmation, PeerConfirmation::Deferred);
        connected.io.write_all(b"live").await.unwrap();
        connected.io.flush().await.unwrap();
        let mut bytes = [0; 4];
        connected.io.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"live");
        observer.0.store(true, Ordering::SeqCst);
        connected.io.write_all(b"fail").await.unwrap();
        let receipt = scope.next_receipt().await.unwrap();
        let original = receipt.result().unwrap_err();
        assert_eq!(receipt.class(), TaskClass::Auxiliary);
        assert!(!receipt.local_failure_accepted());
        assert_eq!(
            scope
                .registrar()
                .spawn(TaskClass::Auxiliary, async { Ok(()) }),
            Err(SpawnError::Capacity)
        );
        if user_receives {
            let failure = connected.io.read_exact(&mut bytes).await.unwrap_err();
            // VMess's native wrapper forwards the same actual aggregate Arc,
            // retaining the original panic payload/source and decision fields.
            fn original_scope<'a>(
                source: &'a (dyn std::error::Error + 'static),
            ) -> Option<&'a Arc<ChannelError>> {
                if let Some(failure) = source.downcast_ref::<super::wire::SharedFailure>() {
                    return Some(&failure.0);
                }
                if let Some(io) = source.downcast_ref::<std::io::Error>() {
                    if let Some(found) = io.get_ref().and_then(|source| original_scope(source)) {
                        return Some(found);
                    }
                }
                source.source().and_then(original_scope)
            }
            assert!(Arc::ptr_eq(&original, original_scope(&failure).unwrap()));
            assert!(receipt.local_failure_accepted());
        }
        drop(connected);
        let report = owner.lifecycle.shutdown(end()).await.unwrap();
        assert!(report.is_complete(), "{report:?}");
        assert_eq!(report.closed_io.len(), 1);
        assert!(
            receipt.local_failure_accepted(),
            "no-user cleanup persists the same original record before accepting"
        );
        assert_eq!(
            scope
                .registrar()
                .spawn(TaskClass::Auxiliary, async { Ok(()) }),
            Err(SpawnError::Capacity)
        );
        scope.acknowledge(&receipt).unwrap();
        let healthy = scope
            .registrar()
            .spawn(TaskClass::Auxiliary, async { Ok(()) })
            .unwrap();
        let success = scope.next_receipt().await.unwrap();
        assert_eq!(success.id(), healthy);
        assert!(success.result().is_ok());
        scope.acknowledge(&success).unwrap();
        scope.begin_shutdown();
        assert!(scope.next_receipt().await.is_none());
        assert!(scope.is_drained());
        finish.send(()).unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn hosted_vmess_real_grpc_user_error_accepts_the_original_auxiliary_exit() {
    actual_failure(true).await;
}
#[tokio::test]
async fn hosted_vmess_real_grpc_no_user_cleanup_accepts_failure_without_global_revocation() {
    actual_failure(false).await;
}

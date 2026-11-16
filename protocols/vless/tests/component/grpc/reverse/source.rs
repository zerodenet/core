//! The production standard Source borrows the actual gRPC role/native owner.
//! Pending unique IO and interrupted close keep that same logical controller.
use super::*;
use zero_platform_tokio::contract::{
    context::AcceptContext,
    datagram::{BoxDatagramSession, DatagramRequest},
    network::{CarrierContext, UpstreamOpener},
    stream::{BoxStream, DynStreamOutbound},
};
fn denied<T: Send + 'static>() -> BoxFuture<'static, Result<T, ChannelError>> {
    Box::pin(async {
        Err(ChannelError::new(
            Stage::Prepare,
            Reason::Unsupported,
            Impact::Component,
        ))
    })
}
struct Selected {
    role: Arc<dyn DynStreamOutbound>,
    context: OpenContext,
    binding: UpstreamBinding,
    actual: Arc<Mutex<Option<zero_platform_tokio::controlled_carrier::CarrierOrigin>>>,
}
impl UpstreamOpener for Selected {
    fn borrow_stream(
        &self,
        binding: &UpstreamBinding,
        destination: Destination,
        call: CarrierContext,
    ) -> BoxFuture<
        '_,
        Result<zero_platform_tokio::controlled_carrier::loan::StreamLoan, ChannelError>,
    > {
        let valid = binding == &self.binding && call.session.is_none();
        let mut context = self.context.clone();
        context.cancel = call.cancel;
        context.deadline = call.deadline;
        Box::pin(async move {
            assert!(valid);
            let loan = self
                .role
                .connect_carrier(ConnectRequest { destination }, context)
                .await?
                .io;
            *self.actual.lock().unwrap() = Some(loan.owner.original().clone());
            Ok(loan)
        })
    }
    fn open_stream(
        &self,
        _: &UpstreamBinding,
        _: Destination,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        denied()
    }
    fn open_datagram(
        &self,
        _: &UpstreamBinding,
        _: DatagramRequest,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxDatagramSession, ChannelError>> {
        denied()
    }
}
#[tokio::test]
async fn standard_source_keeps_actual_loan_after_interrupted_cleanup_until_unique_view_returns() {
    for flow in [None, Some("zero-aead-v1")] {
        timeout(Duration::from_secs(8), async {
            let runtime = crate::transport::VlessTransportRuntime::default();
            let grpc = OwnedGrpcProfile {
                service_names: vec!["actual".into()],
                multi_mode: true,
                ..Default::default()
            };
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let peer = listener.local_addr().unwrap();
            let profile = grpc.clone();
            let (seen, received) = tokio::sync::oneshot::channel();
            let (finish, finished) = tokio::sync::oneshot::channel();
            let server = tokio::spawn(async move {
                let (io, _) = listener.accept().await.unwrap();
                let incoming = zero_transport::grpc::accept_grpc_connection(io, &profile).unwrap();
                let mut stream = incoming.accept().await.unwrap();
                let mut bytes = [0; 19];
                let header = stream.read_exact(&mut bytes);
                tokio::pin!(header);
                std::future::poll_fn(|cx| {
                    assert!(
                        header.as_mut().poll(cx).is_pending(),
                        "Source.next must not poll the Rvs authentication I/O"
                    );
                    Poll::Ready(())
                })
                .await;
                seen.send(()).unwrap();
                finished.await.unwrap();
                drop(header);
                incoming.close();
            });
            let mut input = input(peer.port(), &grpc);
            input.protocol.flow = flow;
            let prepared = PreparedGrpc::from_options_refs(
                input.clone(),
                &runtime,
                None,
                Default::default(),
                NonZeroUsize::new(1).unwrap(),
                NonZeroUsize::new(3).unwrap(),
            )
            .unwrap();
            let mut assembly = Assembly::cold(prepared, peer);
            assembly.start().await;
            let destination = Destination::new(Address::Ipv4([127, 0, 0, 1]), peer.port());
            let binding = UpstreamBinding {
                prefix: vec![RoleInstanceId {
                    component: assembly.role_actual.component,
                    generation: assembly.role_actual.generation,
                    role: RoleId::Outbound(OutboundId(assembly.role_actual.component.0)),
                }],
                ..assembly.upstream.binding().clone()
            };
            let mut context = assembly.context();
            context.identity = RequestIdentity::Service(
                Arc::new(ServiceRequestFacts {
                    origin: ServiceOrigin::Runtime,
                    purpose: ServicePurpose::DialedIngress,
                })
                .into(),
            );
            let actual = Arc::new(Mutex::new(None));
            let upstream = UpstreamTransport::new(
                binding.clone(),
                Arc::new(Selected {
                    role: assembly.role.roles.stream.clone().unwrap(),
                    context,
                    binding,
                    actual: actual.clone(),
                }),
            );
            let options =
                crate::reverse::source::Options::from_options(input.protocol, destination).unwrap();
            let source_id = ComponentId(703);
            let mut instance = crate::reverse::source::Factory
                .build(
                    Arc::new(options),
                    BuildContext {
                        component: source_id,
                        generation: ResourceGeneration(1),
                        dependencies: Arc::from([assembly.role_actual]),
                        roles: ComponentRoleIds {
                            inbound: Some(InboundId(source_id.0)),
                            ..Default::default()
                        },
                        resources: Arc::new(Denied),
                        tasks: assembly.role_scope.registrar().without_tasks(),
                        cancel: CancellationToken::new(),
                        background_carrier: Some(upstream),
                    },
                )
                .unwrap();
            instance.lifecycle.start(deadline()).await.unwrap();
            let mut source = instance.roles.inbound.take().unwrap();
            let pending = source
                .next(AcceptContext {
                    deadline: Some(deadline()),
                    cancel: CancellationToken::new(),
                })
                .await
                .unwrap()
                .unwrap();
            let selected_origin = actual.lock().unwrap().clone().unwrap();
            assert!(pending
                .stream_carrier()
                .unwrap()
                .same_original(&selected_origin));
            let facts = pending.carrier_facts().unwrap();
            assert_eq!(facts.original_source, Some(peer));
            assert!(facts.local_endpoint.is_some());
            received.await.unwrap();
            assert!(assembly.observations.facts.lock().unwrap().is_empty());
            {
                let closing = instance.lifecycle.shutdown(deadline());
                tokio::pin!(closing);
                std::future::poll_fn(|cx| {
                    assert!(
                        closing.as_mut().poll(cx).is_pending(),
                        "held original Source view is not a close receipt"
                    );
                    Poll::Ready(())
                })
                .await;
            }
            // Drop of the close waiter neither loses its real LoanOwner nor closes
            // the physical original still retained by the existing gRPC factory.
            let original = actual.lock().unwrap().clone().unwrap();
            assert_eq!(original.instance(), assembly.actual);
            assert!(original.closed().is_none());
            assert!(!original.driver().unwrap().is_joined());
            drop(pending);
            drop(source);
            assert!(instance
                .lifecycle
                .shutdown(deadline())
                .await
                .unwrap()
                .is_complete());
            let (native, tasks) = assembly.close().await;
            assert!(native.is_complete() && tasks.is_complete());
            assert_eq!(native.closed_io.len(), 1);
            finish.send(()).unwrap();
            server.await.unwrap();
        })
        .await
        .unwrap();
    }
}

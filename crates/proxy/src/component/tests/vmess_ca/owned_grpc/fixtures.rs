use super::*;
use zero_platform_tokio::{controlled_carrier::NativeCarrierOpener, EgressInterfaceControl};
pub(super) fn profile(multi_mode: bool) -> OwnedGrpcProfile {
    OwnedGrpcProfile {
        service_names: vec!["owned.vmess/Tun".into()],
        multi_mode,
        ..Default::default()
    }
}
pub(super) fn input<'a>(
    peer: std::net::SocketAddr,
    tls: Option<&'a OwnedClientTlsProfile>,
    grpc: &'a OwnedGrpcProfile,
) -> vmess::transport::VmessOutboundBuildOptionsRef<
    'a,
    OwnedClientTlsProfile,
    OwnedWebSocketProfile,
    OwnedGrpcProfile,
> {
    vmess::transport::VmessOutboundBuildOptionsRef {
        tag: "owned-grpc",
        server: "127.0.0.1",
        port: peer.port(),
        protocol: vmess::transport::VmessOutboundOptionsRef {
            id: ID,
            cipher: "aes-128-gcm",
            mux_concurrency: None,
            mux_idle_timeout_secs: None,
            mux_response_backlog_frames: None,
            mux_response_backlog_bytes: None,
        },
        tls,
        ws: None,
        grpc: Some(grpc),
    }
}
pub(super) struct Fixture {
    pub(super) owner: ComponentInstance,
    pub(super) scope: TaskScope,
    pub(super) grants: Arc<HostResources>,
    pub(super) observations: Arc<Observations>,
    pub(super) upstream: UpstreamTransport,
}
impl Fixture {
    pub(super) fn cold(
        peer: std::net::SocketAddr,
        runtime: &vmess::transport::VmessTransportRuntime,
        tls: Option<&OwnedClientTlsProfile>,
        grpc: &OwnedGrpcProfile,
        base: Option<&std::path::Path>,
        budgets: (usize, usize),
        observer: Option<Arc<dyn zero_traits::IoObserver>>,
    ) -> Self {
        let (bound, cap) = budgets;
        let cap = NonZeroUsize::new(cap).unwrap();
        let options = vmess::component::grpc::Options::from_options_refs_at(
            input(peer, tls, grpc),
            Default::default(),
            base,
            NonZeroUsize::new(bound),
            cap,
            runtime,
        )
        .unwrap();
        let spec = vmess::component::grpc::Factory.describe(&options);
        assert_eq!(spec.task_limit, Some(cap));
        assert!(spec
            .resources
            .contains(&ResourceDeclaration::BackgroundCarrier));
        let observations = Arc::new(Observations::default());
        let cancel = CancellationToken::new();
        // Two distinct real grants: native File owner and explicit library
        // BackgroundCarrier. HostResources does not claim to implement the latter.
        let files: Vec<_> = spec
            .resources
            .iter()
            .filter(|resource| matches!(resource, ResourceDeclaration::File { .. }))
            .cloned()
            .collect();
        let grants = HostResources::new(ComponentId(1301), &files, cancel.clone()).unwrap();
        let resources = Arc::new(super::super::preparation::ObservedAccess {
            grants: grants.clone(),
            observations: observations.clone(),
        });
        let scope = TaskScope::new(tokio::runtime::Handle::current(), cap);
        let network = Arc::new(EgressInterfaceControl::default());
        let binding = UpstreamBinding {
            prefix: vec![],
            dial: Default::default(),
            network_generation: NetworkGeneration(network.generation()),
        };
        let actual = ComponentInstanceId {
            component: ComponentId(1301),
            generation: ResourceGeneration(2),
        };
        let upstream = UpstreamTransport::new(
            binding.clone(),
            Arc::new(
                NativeCarrierOpener::new(
                    actual,
                    binding,
                    peer,
                    network,
                    Some(observer.unwrap_or_else(|| observations.clone())),
                )
                .unwrap(),
            ),
        );
        let owner = vmess::component::grpc::Factory
            .build(
                Arc::new(options),
                BuildContext {
                    component: actual.component,
                    generation: actual.generation,
                    dependencies: Arc::from([]),
                    roles: ComponentRoleIds {
                        outbound: Some(OutboundId(1302)),
                        ..Default::default()
                    },
                    resources,
                    tasks: scope.registrar(),
                    cancel,
                    background_carrier: Some(upstream.clone()),
                },
            )
            .unwrap();
        Self {
            owner,
            scope,
            grants,
            observations,
            upstream,
        }
    }
    pub(super) async fn start(&mut self) {
        let access = self.grants.activate(deadline()).unwrap();
        self.owner.lifecycle.start(deadline()).await.unwrap();
        drop(access);
    }
    pub(super) fn context(&self) -> OpenContext {
        OpenContext {
            identity: RequestIdentity::Service(Arc::new(ServiceRequestFacts {
                origin: ServiceOrigin::Runtime,
                purpose: ServicePurpose::DiagnosticProbe,
            })),
            deadline: Some(deadline()),
            cancel: CancellationToken::new(),
            observer: Some(self.observations.clone()),
            upstream: self.upstream.clone(),
        }
    }
    pub(super) async fn close(
        &mut self,
    ) -> (
        zero_platform_tokio::contract::lifecycle::CloseReport,
        zero_platform_tokio::contract::lifecycle::CloseReport,
    ) {
        let (owner, tasks) = tokio::join!(
            self.owner.lifecycle.shutdown(deadline()),
            self.scope.shutdown(deadline())
        );
        let owner = owner.unwrap();
        assert!(owner.is_complete(), "{owner:?}");
        assert!(tasks.is_complete(), "{tasks:?}");
        assert!(self
            .grants
            .shutdown_io(deadline())
            .await
            .unwrap()
            .is_complete());
        (owner, tasks)
    }
}

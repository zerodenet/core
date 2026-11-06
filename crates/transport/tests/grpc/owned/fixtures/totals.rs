//! Same actual cold provider and root-budget dependency.
use super::*;
impl Fixture {
    pub(in crate::grpc::owned::tests) fn with_totals(
        mut self,
        root: &TotalsOptions,
        root_instance: ComponentInstanceId,
        instance: ComponentInstanceId,
    ) -> Self {
        let options = Arc::new(
            self.options
                .as_ref()
                .clone()
                .with_root_budget(root_instance.component, root.proposal())
                .unwrap(),
        );
        let peer = std::net::SocketAddr::new(
            options.settings.server.parse().unwrap(),
            options.settings.port,
        );
        let upstream = UpstreamTransport::new(
            self.binding.clone(),
            Arc::new(
                NativeCarrierOpener::new(
                    instance,
                    self.binding.clone(),
                    peer,
                    self.egress.clone(),
                    Some(self.traffic.clone()),
                )
                .unwrap(),
            ),
        );
        let cancel = CancellationToken::new();
        let tasks = self.scope.registrar();
        let owner = NativePoolFactory
            .build(
                options.clone(),
                BuildContext {
                    component: instance.component,
                    generation: instance.generation,
                    dependencies: Arc::from([root_instance]),
                    roles: Default::default(),
                    resources: granted(&options, instance, &tasks, &cancel),
                    tasks,
                    cancel,
                    background_carrier: Some(upstream),
                },
            )
            .unwrap();
        // The previous fixture owner never started or acquired native I/O.
        self.owner = owner;
        self.actual = instance;
        self.options = options;
        self
    }
}

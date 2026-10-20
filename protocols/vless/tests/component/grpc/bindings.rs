use super::*;
#[tokio::test]
async fn real_dependency_generation_and_complete_recipe_are_required_before_role_start() {
    let runtime = crate::transport::VlessTransportRuntime::default();
    let grpc = OwnedGrpcProfile {
        service_names: vec!["actual".into()],
        ..Default::default()
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer = listener.local_addr().unwrap();
    let mut assembly = Assembly::cold(prepare(&runtime, peer.port(), &grpc), peer);
    assembly.start().await;
    let options = Arc::new(prepare(&runtime, peer.port(), &grpc).bind(assembly.actual.component));
    let wrong = ComponentInstanceId {
        generation: ResourceGeneration(2),
        ..assembly.actual
    };
    let scope = TaskScope::empty(tokio::runtime::Handle::current());
    let mut role = StreamFactory
        .build(
            options,
            BuildContext {
                component: ComponentId(703),
                generation: ResourceGeneration(1),
                dependencies: Arc::from([wrong]),
                roles: ComponentRoleIds::default(),
                resources: Arc::new(Denied),
                tasks: scope.registrar(),
                cancel: CancellationToken::new(),
                background_carrier: None,
            },
        )
        .unwrap();
    assert_eq!(
        role.lifecycle.start(deadline()).await.unwrap_err().reason,
        Reason::Revoked
    );
    let (pool, tasks) = assembly.close().await;
    assert!(pool.is_complete());
    assert!(pool.closed_io.is_empty());
    assert!(tasks.exited_tasks.is_empty());
}
#[tokio::test]
async fn source_runtime_drop_does_not_create_a_replacement_pool_for_a_cold_weak_recipe() {
    let runtime = crate::transport::VlessTransportRuntime::default();
    let grpc = OwnedGrpcProfile {
        service_names: vec!["actual".into()],
        ..Default::default()
    };
    let prepared = prepare(&runtime, 443, &grpc);
    let options = prepared.pool_options().clone();
    drop(runtime);
    let scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(1).unwrap(),
    );
    let actual = ComponentInstanceId {
        component: ComponentId(704),
        generation: ResourceGeneration(1),
    };
    let egress = Arc::new(EgressInterfaceControl::default());
    let binding = UpstreamBinding {
        prefix: vec![],
        dial: Default::default(),
        network_generation: NetworkGeneration(egress.generation()),
    };
    let native = NativeCarrierOpener::new(
        actual,
        binding.clone(),
        "127.0.0.1:443".parse().unwrap(),
        egress,
        None,
    )
    .unwrap();
    let result = PoolFactory.build(
        Arc::new(options),
        BuildContext {
            component: actual.component,
            generation: actual.generation,
            dependencies: Arc::from([]),
            roles: ComponentRoleIds::default(),
            resources: Arc::new(Denied),
            tasks: scope.registrar(),
            cancel: CancellationToken::new(),
            background_carrier: Some(UpstreamTransport::new(binding, Arc::new(native))),
        },
    );
    assert_eq!(result.err().unwrap().reason, Reason::Revoked);
}

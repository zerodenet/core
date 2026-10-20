use super::*;

#[test]
fn complete_business_recipe_retains_same_actual_runtime_and_explicit_resource_and_cleanup_bounds() {
    let directory = Directory::new();
    let tls = tls_profile();
    let grpc = OwnedGrpcProfile {
        service_names: vec!["same.actual/Tun".into()],
        multi_mode: true,
        ..Default::default()
    };
    let runtime = vmess::transport::VmessTransportRuntime::default();
    let binding = UpstreamBinding {
        prefix: vec![],
        dial: Default::default(),
        network_generation: NetworkGeneration(19),
    };
    let prepare = || {
        vmess::component::grpc::Options::from_business_options_refs_at(
            input(443, &tls, &grpc),
            binding.clone(),
            Some(&directory.0),
            NonZeroUsize::new(4096),
            one(),
            Duration::from_secs(2),
            &runtime,
        )
        .unwrap()
    };
    let first = prepare();
    let second = prepare();
    let first = vmess::component::grpc::Factory.describe(&first);
    let second = vmess::component::grpc::Factory.describe(&second);
    assert_eq!(first, second);
    assert_eq!(first.task_limit, Some(one()));
    assert_eq!(
        first.resources[0],
        ResourceDeclaration::File {
            path: directory.0.join("identity.pem"),
            max_bytes: 4096
        }
    );
    assert_eq!(
        first.resources[1],
        ResourceDeclaration::ControlledOutboundStream {
            endpoint: Destination::new(Address::Ipv4([127, 0, 0, 1]), 443),
            binding,
            demand: ControlledStreamDemandClass::Business,
            max_sockets: one()
        }
    );
    assert!(
        !directory.0.join("identity.pem").exists(),
        "cold recipe must not open the actual CA file"
    );
}

#[test]
fn same_actual_runtime_rejects_changed_business_bindings_and_retirement_budgets_instead_of_parallel_owners(
) {
    let directory = Directory::new();
    let tls = tls_profile();
    let grpc = OwnedGrpcProfile {
        service_names: vec!["same.actual/Tun".into()],
        ..Default::default()
    };
    for change in 0..3 {
        let runtime = vmess::transport::VmessTransportRuntime::default();
        let binding = UpstreamBinding {
            prefix: vec![],
            dial: Default::default(),
            network_generation: NetworkGeneration(19),
        };
        vmess::component::grpc::Options::from_business_options_refs_at(
            input(443, &tls, &grpc),
            binding.clone(),
            Some(&directory.0),
            NonZeroUsize::new(4096),
            one(),
            Duration::from_secs(2),
            &runtime,
        )
        .unwrap();
        let mut changed = binding;
        let (cap, cleanup) = match change {
            0 => {
                changed.network_generation = NetworkGeneration(20);
                (one(), Duration::from_secs(2))
            }
            1 => (NonZeroUsize::new(2).unwrap(), Duration::from_secs(2)),
            _ => (one(), Duration::from_secs(3)),
        };
        assert!(
            vmess::component::grpc::Options::from_business_options_refs_at(
                input(443, &tls, &grpc),
                changed,
                Some(&directory.0),
                NonZeroUsize::new(4096),
                cap,
                cleanup,
                &runtime
            )
            .is_err()
        );
    }
}

#[test]
fn business_recipe_refuses_a_prefix_and_zero_cleanup_without_granting_a_background_carrier() {
    let tls = tls_profile();
    let grpc = OwnedGrpcProfile::default();
    let binding = UpstreamBinding {
        prefix: vec![],
        dial: Default::default(),
        network_generation: NetworkGeneration(19),
    };
    assert!(
        vmess::component::grpc::Options::from_business_options_refs_at(
            input(443, &tls, &grpc),
            binding.clone(),
            None,
            NonZeroUsize::new(4096),
            one(),
            Duration::ZERO,
            &vmess::transport::VmessTransportRuntime::default()
        )
        .is_err()
    );
    let mut selected = binding;
    selected.prefix.push(RoleInstanceId {
        component: ComponentId(1),
        generation: ResourceGeneration(1),
        role: RoleId::Outbound(OutboundId(1)),
    });
    assert!(
        vmess::component::grpc::Options::from_business_options_refs_at(
            input(443, &tls, &grpc),
            selected,
            None,
            NonZeroUsize::new(4096),
            one(),
            Duration::from_secs(2),
            &vmess::transport::VmessTransportRuntime::default()
        )
        .is_err()
    );
}

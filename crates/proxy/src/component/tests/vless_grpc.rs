//! Cold assembly proof only; real controlled TCP/TLS/H2 tests live with the owner.
#![cfg(feature = "vless")]
use crate::component::{catalog::ComponentCatalog, ComponentRecipe, ResourcePreparation};
use std::{num::NonZeroUsize, sync::Arc};
use vless::{
    component::grpc::{PoolFactory, PreparedGrpc, StreamFactory},
    transport::{VlessOutboundBuildOptionsRef, VlessOutboundOptionsRef, VlessTransportRuntime},
};
use zero_platform_tokio::contract::component::{ComponentFactory, ResourceDeclaration, RoleKind};
use zero_transport::profile::{
    OwnedClientTlsProfile, OwnedGrpcProfile, OwnedH2Profile, OwnedHttpUpgradeProfile,
    OwnedSplitHttpProfile, OwnedWebSocketProfile,
};
fn prepared(runtime: &VlessTransportRuntime, grpc: &OwnedGrpcProfile) -> PreparedGrpc {
    PreparedGrpc::from_options_refs(
        VlessOutboundBuildOptionsRef {
            tag: "canonical-grpc",
            server: "127.0.0.1",
            port: 443,
            protocol: VlessOutboundOptionsRef {
                encryption: None,
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
            grpc: Some(grpc),
            h2: None::<&OwnedH2Profile>,
            http_upgrade: None::<&OwnedHttpUpgradeProfile>,
            split_http: None::<&OwnedSplitHttpProfile>,
            final_mask: None,
            mkcp: None,
            hysteria: None,
            download: None,
        },
        runtime,
        None,
        Default::default(),
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(4).unwrap(),
    )
    .unwrap()
}
#[test]
fn two_vless_consumers_use_real_host_canonical_roleless_grpc_pool_with_complete_profile_identity() {
    let runtime = VlessTransportRuntime::default();
    let grpc = OwnedGrpcProfile {
        service_names: vec!["actual".into()],
        multi_mode: true,
        ..Default::default()
    };
    let ids = ComponentCatalog::default();
    let mut allocate = || ids.allocate_id();
    let mut preparation = ResourcePreparation::new(&mut allocate);
    let mut recipes = Vec::new();
    let mut dependencies = Vec::new();
    for _ in 0..2 {
        let prepared = prepared(&runtime, &grpc);
        let dependency = preparation
            .share(
                "vless.grpc.pool",
                Arc::new(PoolFactory),
                Arc::new(prepared.pool_options().clone()),
            )
            .unwrap();
        dependencies.push(dependency);
        let options = Arc::new(prepared.bind(dependency));
        let spec = StreamFactory.describe(&options);
        assert_eq!(spec.roles, vec![RoleKind::Stream]);
        assert_eq!(spec.dependencies, vec![dependency]);
        assert!(spec.resources.is_empty() && spec.task_limit.is_none());
        recipes.push((
            ids.allocate_id().unwrap(),
            ComponentRecipe::from_validated(Arc::new(StreamFactory), options),
        ));
    }
    assert_eq!(dependencies[0], dependencies[1]);
    let changed = OwnedGrpcProfile {
        authority: Some("different.example:443".into()),
        ..grpc.clone()
    };
    let different = prepared(&runtime, &changed);
    let different_id = preparation
        .share(
            "vless.grpc.pool",
            Arc::new(PoolFactory),
            Arc::new(different.pool_options().clone()),
        )
        .unwrap();
    assert_ne!(different_id, dependencies[0]);
    let order = preparation
        .validate(recipes.iter().map(|(id, recipe)| (*id, recipe.spec())))
        .unwrap();
    let resources = preparation.into_resources().unwrap();
    assert_eq!(resources.len(), 2);
    for resource in &resources {
        assert!(resource.recipe().spec().roles.is_empty());
        assert_eq!(
            resource.recipe().spec().resources,
            vec![ResourceDeclaration::BackgroundCarrier]
        );
        assert_eq!(resource.recipe().spec().task_limit, NonZeroUsize::new(2));
    }
    let dependency_index = order.iter().position(|id| *id == dependencies[0]).unwrap();
    assert!(recipes.iter().all(|(id, _)| order
        .iter()
        .position(|candidate| candidate == id)
        .unwrap()
        > dependency_index));
    let old = PoolFactory
        .describe(prepared(&runtime, &grpc).pool_options())
        .static_identity;
    runtime.on_config_reloaded();
    let after = PoolFactory
        .describe(prepared(&runtime, &grpc).pool_options())
        .static_identity;
    assert_ne!(
        old, after,
        "retired actual state cannot be reused by equal config after reload"
    );
}

use super::*;
#[tokio::test]
async fn actual_runtime_reload_revokes_retained_native_recipe_and_owner_bound_role() {
    let runtime = crate::transport::VlessTransportRuntime::default();
    let grpc = OwnedGrpcProfile {
        service_names: vec!["actual".into()],
        ..Default::default()
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer = listener.local_addr().unwrap();
    let prepared = prepare(&runtime, peer.port(), &grpc);
    let mut assembly = Assembly::cold(prepared, peer);
    assembly.start().await;
    runtime.on_config_reloaded();
    let failure = assembly
        .role
        .roles
        .stream
        .as_ref()
        .unwrap()
        .connect(
            ConnectRequest {
                destination: destination(),
            },
            assembly.context(),
        )
        .await
        .err()
        .unwrap();
    assert_eq!(failure.reason, Reason::Revoked);
    assert!(failure.session.is_none());
    let fresh = prepare(&runtime, peer.port(), &grpc);
    let old_identity = PoolFactory.describe(&assembly.pool_options).static_identity;
    let fresh_identity = PoolFactory.describe(fresh.pool_options()).static_identity;
    assert_ne!(
        old_identity, fresh_identity,
        "ABA config creates a new actual pool after retirement"
    );
    let (pool, tasks) = assembly.close().await;
    assert!(pool.is_complete());
    assert!(pool.closed_io.is_empty());
    assert!(tasks.exited_tasks.is_empty());
}

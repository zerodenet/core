use super::*;
#[tokio::test]
async fn legacy_reload_cannot_revoke_a_native_role_but_its_actual_owner_retirement_does() {
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
    let connected = assembly
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
        .unwrap();
    drop(connected);
    assembly
        .role
        .lifecycle
        .revoke(zero_platform_tokio::contract::CloseReason::Shutdown);
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
    assert_eq!(
        old_identity, fresh_identity,
        "the same cold native identity is independent of the retired library cache hook"
    );
    let (pool, tasks) = assembly.close().await;
    assert!(pool.is_complete());
    assert!(pool.closed_io.is_empty());
    assert!(tasks.exited_tasks.is_empty());
}

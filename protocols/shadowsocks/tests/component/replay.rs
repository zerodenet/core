use super::*;
use std::{
    error::Error,
    future::Future,
    task::{Context, Poll, Waker},
    time::Duration,
};
use zero_core::{
    component::{ComponentRoleIds, ResourceGeneration},
    request::Destination,
};
use zero_platform_tokio::contract::{
    component::ResourceAccess, packet::BoxPacketSession, scope::TaskScope, BoxFuture,
};

struct NoResources;
fn denied<T: Send + 'static>() -> BoxFuture<'static, Result<T, BuildError>> {
    Box::pin(async {
        Err(ChannelError::new(
            Stage::Prepare,
            Reason::Internal,
            Impact::Component,
        ))
    })
}
impl ResourceAccess for NoResources {
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
    ) -> BoxFuture<'_, Result<tokio::net::TcpListener, BuildError>> {
        denied()
    }
    fn listen_datagram(
        &self,
        _: Destination,
    ) -> BoxFuture<'_, Result<tokio::net::UdpSocket, BuildError>> {
        denied()
    }
    fn open_device(&self, _: Arc<str>) -> BoxFuture<'_, Result<BoxPacketSession, BuildError>> {
        denied()
    }
}
fn context(limit: usize) -> (TaskScope, BuildContext) {
    let runtime = tokio::runtime::Handle::current();
    let scope = match NonZeroUsize::new(limit) {
        Some(limit) => TaskScope::new(runtime, limit),
        None => TaskScope::empty(runtime),
    };
    let context = BuildContext {
        component: ComponentId(44),
        generation: ResourceGeneration(1),
        roles: ComponentRoleIds::default(),
        resources: Arc::new(NoResources),
        tasks: scope.registrar(),
        cancel: CancellationToken::new(),
        background_carrier: None,
    };
    (scope, context)
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(2)
}

#[tokio::test]
async fn replay_factory_transfers_the_actual_shared_history_to_one_registered_driver() {
    let state = crate::transport::ShadowsocksTransportState::default();
    let options = Arc::new(state.replay_component_options(ReplayPolicy::Reject, Some(2)));
    let same = state.replay_component_options(ReplayPolicy::Reject, Some(2));
    assert!(Weak::ptr_eq(&options.pool, &same.pool));
    let pool = options.pool.upgrade().unwrap();
    pool.check_and_insert(&[1; 16]).unwrap();
    pool.check_and_insert(&[2; 16]).unwrap();
    tokio::task::yield_now().await;
    let spec = ReplayFactory.describe(&options);
    assert!(spec.roles.is_empty());
    assert_eq!(spec.task_limit, NonZeroUsize::new(1));
    let (mut scope, ctx) = context(1);
    let registrar = scope.registrar();
    let mut instance = ReplayFactory.build(options, ctx).unwrap();
    assert!(
        instance.roles.stream.is_none()
            && instance.roles.datagram.is_none()
            && instance.roles.packet.is_none()
            && instance.roles.inbound.is_none()
    );
    instance.lifecycle.start(deadline()).await.unwrap();
    instance.lifecycle.start(deadline()).await.unwrap();
    assert!(pool.check_and_insert(&[1; 16]).is_err());
    assert!(pool.check_and_insert(&[3; 16]).is_err());
    assert_eq!(
        registrar.spawn(TaskClass::Auxiliary, async { Ok(()) }),
        Err(SpawnError::Capacity)
    );
    let end = deadline();
    let (protocol, tasks) = tokio::join!(instance.lifecycle.shutdown(end), scope.shutdown(end));
    assert!(protocol.unwrap().is_complete());
    assert!(tasks.is_complete());
    assert_eq!(tasks.exited_tasks.len(), 1);
    assert!(tasks.failed_tasks.is_empty());
    assert!(
        pool.check_and_insert(&[1; 16]).is_err(),
        "cleanup must not reset valid replay state"
    );
}

#[tokio::test]
async fn registration_failure_keeps_the_original_capacity_source_and_security_history() {
    let state = crate::transport::ShadowsocksTransportState::default();
    let options = Arc::new(state.replay_component_options(ReplayPolicy::Reject, Some(2)));
    let pool = options.pool.upgrade().unwrap();
    pool.check_and_insert(&[7; 16]).unwrap();
    let (mut scope, ctx) = context(0);
    let mut instance = ReplayFactory.build(options, ctx).unwrap();
    let error = instance.lifecycle.start(deadline()).await.unwrap_err();
    assert_eq!(error.reason, Reason::Capacity);
    assert_eq!(
        error.source().unwrap().downcast_ref::<SpawnError>(),
        Some(&SpawnError::Capacity)
    );
    assert!(pool.check_and_insert(&[7; 16]).is_err());
    let end = deadline();
    let (protocol, tasks) = tokio::join!(instance.lifecycle.shutdown(end), scope.shutdown(end));
    assert!(protocol.unwrap().is_complete());
    assert!(tasks.is_complete());
    assert!(tasks.exited_tasks.is_empty());
}

#[tokio::test]
async fn interrupted_legacy_driver_transfer_retains_the_join_handle_until_cleanup() {
    let state = crate::transport::ShadowsocksTransportState::default();
    let options = state.replay_component_options(ReplayPolicy::Reject, Some(2));
    let pool = options.pool.upgrade().unwrap();
    pool.check_and_insert(&[1; 16]).unwrap();
    tokio::task::yield_now().await;
    let (mut scope, ctx) = context(1);
    let mut owner = Owner::new(pool.clone(), ctx);
    let mut start = Box::pin(owner.start(deadline()));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(matches!(start.as_mut().poll(&mut cx), Poll::Pending));
    drop(start);
    let legacy = owner
        .legacy
        .as_ref()
        .expect("persistent legacy join owner")
        .abort_handle();
    let end = deadline();
    let (protocol, tasks) = tokio::join!(owner.shutdown(end), scope.shutdown(end));
    assert!(protocol.unwrap().is_complete());
    assert!(tasks.is_complete());
    assert!(legacy.is_finished());
    assert!(pool.check_and_insert(&[1; 16]).is_err());
}

#[tokio::test]
async fn retired_weak_recipe_cannot_keep_security_state_alive() {
    let state = crate::transport::ShadowsocksTransportState::default();
    let options = Arc::new(state.replay_component_options(ReplayPolicy::Reject, Some(2)));
    drop(state);
    assert!(options.pool.upgrade().is_none());
    let (_scope, ctx) = context(1);
    assert_eq!(
        ReplayFactory.build(options, ctx).err().unwrap().reason,
        Reason::Revoked
    );
}

#[tokio::test]
async fn a_replacement_cannot_claim_the_pool_before_the_old_registered_task_exits() {
    let state = crate::transport::ShadowsocksTransportState::default();
    let options = Arc::new(state.replay_component_options(ReplayPolicy::Reject, Some(2)));
    let pool = options.pool.upgrade().unwrap();
    let (mut old_scope, ctx) = context(1);
    let mut old = ReplayFactory.build(options.clone(), ctx).unwrap();
    old.lifecycle.start(deadline()).await.unwrap();
    // This hook requests cancellation. Only the real task join proves exit.
    assert!(old
        .lifecycle
        .shutdown(deadline())
        .await
        .unwrap()
        .is_complete());
    let (mut blocked_scope, ctx) = context(1);
    let mut blocked = ReplayFactory.build(options.clone(), ctx).unwrap();
    let result = blocked.lifecycle.start(deadline()).await;
    assert!(
        matches!(result, Err(ref error) if error.reason == Reason::StalePlan),
        "protocol cleanup alone released a still-running registered worker"
    );
    let end = deadline();
    let (protocol, tasks) =
        tokio::join!(blocked.lifecycle.shutdown(end), blocked_scope.shutdown(end));
    assert!(protocol.unwrap().is_complete());
    assert!(tasks.is_complete());
    let report = old_scope.shutdown(deadline()).await;
    assert!(report.is_complete());
    assert_eq!(report.exited_tasks.len(), 1);
    let (mut scope, ctx) = context(1);
    let mut replacement = ReplayFactory.build(options, ctx).unwrap();
    replacement.lifecycle.start(deadline()).await.unwrap();
    pool.check_and_insert(&[4; 16]).unwrap();
    let end = deadline();
    let (protocol, tasks) = tokio::join!(replacement.lifecycle.shutdown(end), scope.shutdown(end));
    assert!(protocol.unwrap().is_complete());
    assert!(tasks.is_complete());
    assert_eq!(tasks.exited_tasks.len(), 1);
    assert!(pool.check_and_insert(&[4; 16]).is_err());
}

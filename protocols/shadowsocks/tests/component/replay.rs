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
fn context(limit: usize) -> (TaskScope, BuildContext) {
    let runtime = tokio::runtime::Handle::current();
    let scope = match NonZeroUsize::new(limit) {
        Some(limit) => TaskScope::new(runtime, limit),
        None => TaskScope::empty(runtime),
    };
    let context = BuildContext {
        component: ComponentId(44),
        generation: ResourceGeneration(1),
        dependencies: Arc::from([]),
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
    assert!(options.pool.same_history(&same.pool));
    let pool = options.pool.upgrade().unwrap();
    pool.check_and_insert(&[1; 16]).unwrap();
    pool.check_and_insert(&[2; 16]).unwrap();
    tokio::task::yield_now().await;
    let spec = ReplayFactory.describe(&options);
    assert!(spec.roles.is_empty());
    assert_eq!(spec.task_limit, NonZeroUsize::new(1));
    let (mut scope, ctx) = context(1);
    let registrar = scope.registrar();
    let actual = ctx.instance();
    assert!(!pool.registered_owner_is_ready(actual));
    let mut instance = ReplayFactory.build(options, ctx).unwrap();
    assert!(
        instance.roles.stream.is_none()
            && instance.roles.datagram.is_none()
            && instance.roles.packet.is_none()
            && instance.roles.inbound.is_none()
    );
    instance.lifecycle.start(deadline()).await.unwrap();
    assert!(pool.registered_owner_is_ready(actual));
    assert!(
        !pool.registered_owner_is_ready(zero_core::component::ComponentInstanceId {
            generation: ResourceGeneration(actual.generation.0 + 1),
            ..actual
        })
    );
    instance.lifecycle.start(deadline()).await.unwrap();
    assert!(pool.check_and_insert(&[1; 16]).is_err());
    assert!(pool.check_and_insert(&[3; 16]).is_err());
    assert_eq!(
        registrar.spawn(TaskClass::Auxiliary, async { Ok(()) }),
        Err(SpawnError::Capacity)
    );
    instance.lifecycle.revoke(CloseReason::Shutdown);
    assert!(
        !pool.registered_owner_is_ready(actual),
        "revocation is immediate, before worker join"
    );
    let end = deadline();
    let (protocol, tasks) = tokio::join!(instance.lifecycle.shutdown(end), scope.shutdown(end));
    assert!(protocol.unwrap().is_complete());
    assert!(tasks.is_complete());
    assert_eq!(tasks.exited_tasks.len(), 1);
    assert!(tasks.failed_tasks.is_empty());
    assert!(!pool.registered_owner_is_ready(actual));
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

#[test]
fn canonical_replay_recipe_matches_the_actual_history_and_not_only_its_equal_limits() {
    let state = crate::transport::ShadowsocksTransportState::default();
    let first = state.replay_component_options(ReplayPolicy::Reject, Some(2));
    let same = state.replay_component_options(ReplayPolicy::Reject, Some(2));
    let independent = crate::transport::ShadowsocksTransportState::default()
        .replay_component_options(ReplayPolicy::Reject, Some(2));
    assert_eq!(
        ReplayFactory.describe(&first).static_identity,
        ReplayFactory.describe(&same).static_identity
    );
    assert_ne!(
        ReplayFactory.describe(&first).static_identity,
        ReplayFactory.describe(&independent).static_identity
    );
    assert!(first.pool.same_history(&same.pool));
    assert!(!first.pool.same_history(&independent.pool));
}

#[tokio::test]
async fn total_limit_installs_only_after_the_actual_replay_scope_is_ready() {
    let budget = crate::shared::ReplayBudget::new(Some(3)).unwrap();
    let state = crate::transport::ShadowsocksTransportState::with_replay_budget(budget.clone());
    let existing = state.replay_component_options(ReplayPolicy::Reject, Some(2));
    let original = existing.pool().unwrap();
    original.check_and_insert(&[1; 16]).unwrap();
    let candidate = Arc::new(original.with_total(Some(1)).unwrap());
    assert_eq!(
        budget.limit(),
        Some(3),
        "cold recipe cannot shrink live admission"
    );
    let options = Arc::new(ReplayOptions::new(candidate, ReplayPolicy::Reject, Some(2)));
    let (mut failed_scope, ctx) = context(0);
    let mut failed = ReplayFactory.build(options.clone(), ctx).unwrap();
    assert_eq!(
        failed.lifecycle.start(deadline()).await.unwrap_err().reason,
        Reason::Capacity
    );
    assert_eq!(
        budget.limit(),
        Some(3),
        "failed task registration cannot commit a limit"
    );
    let end = deadline();
    let (protocol, tasks) =
        tokio::join!(failed.lifecycle.shutdown(end), failed_scope.shutdown(end));
    assert!(protocol.unwrap().is_complete());
    assert!(tasks.is_complete());
    let (mut scope, ctx) = context(1);
    let mut live = ReplayFactory.build(options, ctx).unwrap();
    live.lifecycle.start(deadline()).await.unwrap();
    assert_eq!(budget.limit(), Some(1));
    assert!(original.check_and_insert(&[2; 16]).is_err());
    let end = deadline();
    let (protocol, tasks) = tokio::join!(live.lifecycle.shutdown(end), scope.shutdown(end));
    assert!(protocol.unwrap().is_complete());
    assert!(tasks.is_complete());
    assert_eq!(tasks.exited_tasks.len(), 1);
    assert_eq!(
        budget.retained(),
        1,
        "joined worker does not erase security history"
    );
    assert!(original.check_and_insert(&[1; 16]).is_err());
}

#[test]
fn mixed_capacity_and_policy_consumers_request_one_actual_maintenance_owner() {
    let state = crate::transport::ShadowsocksTransportState::default();
    let first = state.replay_component_options(ReplayPolicy::Reject, Some(1));
    let second = state.replay_component_options(ReplayPolicy::Ignore, Some(2));
    assert!(first.pool.same_history(&second.pool));
    assert_eq!(
        ReplayFactory.describe(&first),
        ReplayFactory.describe(&second)
    );
    first.pool().unwrap().check_and_insert(&[61; 16]).unwrap();
    assert!(second.pool().unwrap().check_and_insert(&[61; 16]).is_err());
    second.pool().unwrap().check_and_insert(&[62; 16]).unwrap();
}

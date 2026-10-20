//! Explicit owner for one shared TCP2022 replay-maintenance resource.
//! Core activation waits for shared-resource planning and replacement ordering.
use crate::{shared::ReplaySaltPool, validation::ReplayPolicy};
use std::{
    num::NonZeroUsize,
    sync::{Arc, Weak},
};
use tokio::{
    task::JoinHandle,
    time::{timeout_at, Instant},
};
use zero_core::component::{ComponentId, ComponentInstanceId};
use zero_platform_tokio::contract::{
    component::{
        BuildContext, ComponentFactory, ComponentInstance, ComponentRoles, ComponentSpec,
        ResourceOwner,
    },
    lifecycle::{
        erase_lifecycle, BuildError, CloseReport, ResourceLifecycle, TaskClass, Unfinished,
    },
    scope::{SpawnError, TaskRegistrar},
    CancellationToken, ChannelError, CloseReason, Impact, Reason, Stage,
};

/// A weak recipe referencing the actual adapter-scoped security history.
/// Retired recipes cannot keep a host's salt storage alive.
#[derive(Debug, Clone)]
pub struct ReplayOptions {
    pool: Weak<ReplaySaltPool>,
    identity: Arc<[u8]>,
    owner: Arc<str>,
}
impl ReplayOptions {
    pub(crate) fn new(
        pool: Arc<ReplaySaltPool>,
        policy: ReplayPolicy,
        capacity: Option<usize>,
    ) -> Self {
        let owner: Arc<str> = format!("shadowsocks.replay.{policy:?}.{capacity:?}").into();
        Self {
            pool: Arc::downgrade(&pool),
            identity: owner.as_bytes().into(),
            owner,
        }
    }
}
pub struct ReplayFactory;
impl ComponentFactory for ReplayFactory {
    type Options = ReplayOptions;
    fn describe(&self, options: &ReplayOptions) -> ComponentSpec {
        ComponentSpec {
            owner: ResourceOwner::Endpoint(options.owner.clone()),
            roles: vec![],
            dependencies: vec![],
            static_identity: options.identity.clone(),
            resources: vec![],
            task_limit: NonZeroUsize::new(1),
        }
    }
    fn build(
        &self,
        options: Arc<ReplayOptions>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        let pool = options.pool.upgrade().ok_or_else(|| {
            failure(
                context.component,
                Stage::Prepare,
                Reason::Revoked,
                "replay state owner was released",
            )
        })?;
        Ok(ComponentInstance {
            lifecycle: erase_lifecycle(Owner::new(pool, context)),
            roles: ComponentRoles::default(),
        })
    }
}
fn failure(
    component: ComponentId,
    stage: Stage,
    reason: Reason,
    message: &'static str,
) -> ChannelError {
    let mut error = ChannelError::new(stage, reason, Impact::Component)
        .with_source(std::io::Error::other(message));
    error.component = Some(component);
    error
}
struct Owner {
    pool: Option<Arc<ReplaySaltPool>>,
    component: ComponentId,
    instance: ComponentInstanceId,
    tasks: TaskRegistrar,
    cancel: CancellationToken,
    legacy: Option<JoinHandle<()>>,
    claimed: bool,
    attempted: bool,
    started: bool,
}
impl Owner {
    fn new(pool: Arc<ReplaySaltPool>, context: BuildContext) -> Self {
        Self {
            pool: Some(pool),
            component: context.component,
            instance: context.instance(),
            tasks: context.tasks,
            cancel: context.cancel.child_token(),
            legacy: None,
            claimed: false,
            attempted: false,
            started: false,
        }
    }
    async fn join_legacy(&mut self, deadline: Instant, stage: Stage) -> Result<bool, BuildError> {
        let Some(task) = self.legacy.as_mut() else {
            return Ok(true);
        };
        match timeout_at(deadline, task).await {
            Err(_) => Ok(false),
            Ok(result) => {
                drop(self.legacy.take());
                result.map_err(|source| {
                    let mut error = ChannelError::new(stage, Reason::Internal, Impact::Component)
                        .with_source(source);
                    error.component = Some(self.component);
                    error
                })?;
                Ok(true)
            }
        }
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.revoke(CloseReason::Shutdown);
    }
}
impl ResourceLifecycle for Owner {
    async fn start(&mut self, deadline: Instant) -> Result<(), BuildError> {
        if self.started && !self.cancel.is_cancelled() {
            return Ok(());
        }
        if self.attempted || self.cancel.is_cancelled() {
            return Err(failure(
                self.component,
                Stage::Prepare,
                Reason::Revoked,
                "replay maintenance start is not admissible",
            ));
        }
        if Instant::now() >= deadline {
            return Err(failure(
                self.component,
                Stage::Prepare,
                Reason::Timeout,
                "replay maintenance start deadline elapsed",
            ));
        }
        self.attempted = true;
        let pool = self.pool.as_ref().unwrap();
        self.legacy = pool
            .claim_maintenance(Some(self.instance))
            .map_err(|source| {
                failure(
                    self.component,
                    Stage::Prepare,
                    Reason::StalePlan,
                    "replay maintenance owner conflict",
                )
                .with_source(source)
            })?;
        self.claimed = true;
        // The JoinHandle is in this persistent owner before any await. Canceling
        // start cannot orphan the legacy worker during ownership transfer.
        if !self.join_legacy(deadline, Stage::Prepare).await? {
            return Err(failure(
                self.component,
                Stage::Prepare,
                Reason::Timeout,
                "legacy replay maintenance has not exited",
            ));
        }
        if self.cancel.is_cancelled() {
            return Err(failure(
                self.component,
                Stage::Prepare,
                Reason::Revoked,
                "replay maintenance was revoked",
            ));
        }
        if Instant::now() >= deadline {
            return Err(failure(
                self.component,
                Stage::Prepare,
                Reason::Timeout,
                "replay maintenance start deadline elapsed",
            ));
        }
        let job = self.pool.as_ref().unwrap().registered_maintenance(
            self.component,
            self.cancel.clone(),
            self.tasks.cancellation(),
        );
        // The worker future now owns the lease, including an unpolled future
        // rejected by the registrar. This owner must never release it early.
        self.claimed = false;
        self.tasks
            .spawn(TaskClass::Critical, job)
            .map_err(|source| {
                let reason = match source {
                    SpawnError::NotStarted => Reason::InvalidConfig,
                    SpawnError::Capacity => Reason::Capacity,
                    SpawnError::Closing => Reason::Revoked,
                    SpawnError::IdentityExhausted => Reason::Internal,
                };
                let mut error = ChannelError::new(Stage::Prepare, reason, Impact::Component)
                    .with_source(source);
                error.component = Some(self.component);
                error
            })?;
        self.pool
            .as_ref()
            .unwrap()
            .bind_registered_owner(self.instance)
            .map_err(|source| {
                failure(
                    self.component,
                    Stage::Prepare,
                    Reason::Revoked,
                    "registered replay driver has already exited",
                )
                .with_source(source)
            })?;
        self.started = true;
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {
        if let Some(pool) = &self.pool {
            pool.revoke_registered_owner(self.instance);
        }
        self.cancel.cancel();
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, BuildError> {
        self.revoke(CloseReason::Shutdown);
        if !self.join_legacy(deadline, Stage::Close).await? {
            return Ok(CloseReport {
                unfinished: vec![Unfinished::Resource("legacy replay maintenance".into())],
                ..Default::default()
            });
        }
        if self.claimed {
            self.pool
                .as_ref()
                .unwrap()
                .release_maintenance(Some(self.instance));
            self.claimed = false;
        }
        drop(self.pool.take());
        // This driver owns no protocol I/O. The host's real TaskScope join is
        // the completion evidence; never manufacture an exited TaskId here.
        Ok(CloseReport::default())
    }
}

#[cfg(test)]
#[path = "../../tests/component/replay.rs"]
mod tests;

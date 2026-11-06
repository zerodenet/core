//! One actual owner task performs startup, readiness and exit monitoring.
//! Native tree ownership is distinct and will be consumed through its provider.
use super::*;
use std::{error::Error, fmt};
use tokio::sync::oneshot;
use zero_platform_tokio::contract::{
    lifecycle::TaskClass,
    scope::{SpawnError, TaskScopeIdentity},
};
#[derive(Debug)]
struct OriginalFailure(Arc<ChannelError>);
impl fmt::Display for OriginalFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.0.as_ref(), f)
    }
}
impl Error for OriginalFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.0.as_ref())
    }
}
fn same_error(source: Arc<ChannelError>) -> ChannelError {
    let mut error = ChannelError::new(source.stage, source.reason, source.impact);
    error.component = source.component;
    error.session = source.session;
    error.delivery = source.delivery;
    error.retry = source.retry;
    error.with_source(OriginalFailure(source))
}
struct CancelAttempt {
    cancel: CancellationToken,
    resource: ShadowsocksPluginResource,
    armed: bool,
}
impl Drop for CancelAttempt {
    fn drop(&mut self) {
        if self.armed {
            self.cancel.cancel();
            self.resource.revoke();
        }
    }
}
struct Actor {
    component: ComponentId,
    scope: TaskScopeIdentity,
    tasks: TaskRegistrar,
    cancel: CancellationToken,
    scope_cancel: CancellationToken,
    resource: ShadowsocksPluginResource,
    resources: Arc<dyn ResourceAccess>,
    claimed: Arc<AtomicBool>,
    endpoint_ordinal: Option<usize>,
}
impl Actor {
    async fn run(
        self,
        deadline: Instant,
        started: oneshot::Receiver<TaskJoinWitness>,
        ready: oneshot::Sender<Result<(), Arc<ChannelError>>>,
    ) -> Result<(), ChannelError> {
        let outcome=async {
            let witness=started.await.map_err(|source|failure(self.component,Stage::Prepare,Reason::Cancelled,"plugin actor start gate closed").with_source(source))?;
            if witness.is_joined() || !witness.scope_identity().same_scope(&self.scope) || !self.scope.resource_open() {
                return Err(failure(self.component,Stage::Prepare,Reason::StalePlan,"plugin startup caller does not belong to the original live Scope"));
            }
            tokio::select!{biased;
                _=self.cancel.cancelled()=>Err(failure(self.component,Stage::Prepare,Reason::Cancelled,"plugin startup cancelled")),
                _=self.scope_cancel.cancelled()=>Err(failure(self.component,Stage::Prepare,Reason::Revoked,"plugin Scope cancelled")),
                result=tokio::time::timeout_at(deadline,self.resource.start_authorized(self.resources.clone(),self.claimed.clone(),self.tasks.clone(),witness,self.endpoint_ordinal,deadline))=>result.map_err(|source|failure(self.component,Stage::Prepare,Reason::Timeout,"plugin readiness deadline elapsed").with_source(source))?.map(|_|()).map_err(|source|startup_failure(self.component,source)),
            }
        }.await;
        if let Err(source) = outcome {
            self.resource.revoke();
            let source = Arc::new(source);
            let _ = ready.send(Err(source.clone()));
            // Cancellation is normal actor termination; native closure remains
            // owed by the same persistent plan and its actual lifecycle owner.
            return if self.cancel.is_cancelled() || self.scope_cancel.is_cancelled() {
                Ok(())
            } else {
                Err(same_error(source))
            };
        }
        if ready.send(Ok(())).is_err() {
            self.resource.revoke();
            return Ok(());
        }
        tokio::select! {biased;
            _=self.cancel.cancelled()=>Ok(()),
            _=self.scope_cancel.cancelled()=>Ok(()),
            result=self.resource.wait_exit()=>match result {
                Ok(status)=>Err(failure(self.component,Stage::Receive,Reason::Io,"native plugin child exited").with_source(PluginRootExit::new(status))),
                Err(source)=>Err(failure(self.component,Stage::Receive,Reason::Io,"native plugin child status failed").with_source(source)),
            },
        }
    }
}
impl Owner {
    pub(super) async fn start_actor(&mut self, deadline: Instant) -> Result<(), ChannelError> {
        let (start, started) = oneshot::channel();
        let (ready, readied) = oneshot::channel();
        let actor = Actor {
            component: self.component,
            scope: self.tasks.scope_identity(),
            tasks: self.tasks.clone(),
            cancel: self.cancel.clone(),
            scope_cancel: self.tasks.cancellation(),
            resource: self.resource.clone(),
            resources: self.resources.clone(),
            claimed: self.claimed.clone(),
            endpoint_ordinal: self.endpoint_ordinal,
        };
        let (_, witness) = self
            .tasks
            .spawn_tracked(TaskClass::Critical, actor.run(deadline, started, ready))
            .map_err(|source| {
                failure(
                    self.component,
                    Stage::Prepare,
                    match source {
                        SpawnError::NotStarted => Reason::InvalidConfig,
                        SpawnError::Capacity => Reason::Capacity,
                        SpawnError::Closing => Reason::Revoked,
                        SpawnError::IdentityExhausted => Reason::Internal,
                    },
                    "plugin actor registration failed",
                )
                .with_source(source)
            })?;
        // The actual owner retains its original task before any process or
        // readiness I/O can begin; a channel transfers no additional credit.
        self.actor = Some(witness.clone());
        let mut attempt = CancelAttempt {
            cancel: self.cancel.clone(),
            resource: self.resource.clone(),
            armed: true,
        };
        start.send(witness).map_err(|_| {
            failure(
                self.component,
                Stage::Prepare,
                Reason::Cancelled,
                "plugin actor closed before startup",
            )
        })?;
        let outcome = readied.await.map_err(|source| {
            failure(
                self.component,
                Stage::Prepare,
                Reason::Internal,
                "plugin actor ended before readiness",
            )
            .with_source(source)
        })?;
        outcome.map_err(same_error)?;
        attempt.armed = false;
        Ok(())
    }
}

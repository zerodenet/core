//! The original one-task allowance is reserved before process or probe I/O.
use super::*;
use tokio::sync::oneshot;
use zero_platform_tokio::contract::scope::{SpawnError, TaskClass, TaskScopeIdentity};
struct Actor {
    config: PluginConfig,
    shared: Arc<Shared>,
    resources: Arc<dyn ResourceAccess>,
    scope: TaskScopeIdentity,
    tasks: TaskRegistrar,
    scope_cancel: CancellationToken,
}
impl Actor {
    async fn ready(&self, deadline: Instant) -> Result<(), ChannelError> {
        if !self.config.mode.tcp() {
            if let Some(status) = self.shared.status()? {
                return Err(
                    error(Stage::Prepare, Reason::Io, "plugin exited during readiness")
                        .with_source(ChildExited(status)),
                );
            }
            return Ok(());
        }
        loop {
            if let Some(status) = self.shared.status()? {
                return Err(
                    error(Stage::Prepare, Reason::Io, "plugin exited during readiness")
                        .with_source(ChildExited(status)),
                );
            }
            let probe = self.resources.probe_inbound_listener(deadline);
            tokio::pin!(probe);
            let connected = loop {
                tokio::select! {biased;
                    _=self.shared.cancel.cancelled()=>return Err(error(Stage::Prepare,Reason::Cancelled,"plugin start cancelled")),
                    result=&mut probe=>break result?,
                    _=tokio::time::sleep(Duration::from_millis(10))=>{if let Some(status)=self.shared.status()?{return Err(error(Stage::Prepare,Reason::Io,"plugin exited during pending probe").with_source(ChildExited(status)));}},
                }
            };
            if connected {
                if let Some(status) = self.shared.status()? {
                    return Err(
                        error(Stage::Prepare, Reason::Io, "plugin exited after probe")
                            .with_source(ChildExited(status)),
                    );
                }
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(error(
                    Stage::Prepare,
                    Reason::Timeout,
                    "plugin readiness timed out",
                ));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    async fn launch(
        &self,
        witness: TaskJoinWitness,
        deadline: Instant,
    ) -> Result<(), ChannelError> {
        let endpoints = self.resources.inbound_endpoints()?;
        let local = if self.config.mode.tcp() {
            endpoints.stream
        } else {
            endpoints.datagram
        };
        let request = process::request(
            &self.config,
            (
                &endpoints.external.ip().to_string(),
                endpoints.external.port(),
            ),
            local,
            true,
        );
        if self.shared.cancel.is_cancelled() || self.scope_cancel.is_cancelled() {
            return Err(error(
                Stage::Prepare,
                Reason::Cancelled,
                "plugin startup cancelled",
            ));
        }
        #[cfg(target_os = "linux")]
        {
            let original = {
                let mut state = self.shared.state.lock().unwrap();
                let original = self.resources.prepare_owned_process(request, &self.tasks)?;
                state.child = Some(crate::transport::plugin::child::PluginChild::Owned(
                    original.clone(),
                ));
                original
            };
            original
                .start(witness)
                .map_err(|source| io(Stage::Prepare, source))?;
            original
                .wait_started(deadline)
                .await
                .map_err(|source| io(Stage::Prepare, source))
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (witness, deadline, &self.tasks);
            let mut state = self.shared.state.lock().unwrap();
            let child = self.resources.spawn_process(request)?;
            state.pid = child.id();
            state.child = Some(crate::transport::plugin::child::PluginChild::Library(child));
            Ok(())
        }
    }
    async fn run(
        self,
        deadline: Instant,
        started: oneshot::Receiver<TaskJoinWitness>,
        ready: oneshot::Sender<Result<(), Arc<ChannelError>>>,
    ) -> Result<(), ChannelError> {
        let outcome = async {
            let witness = started.await.map_err(|source| error(Stage::Prepare, Reason::Cancelled, "plugin actor start gate closed").with_source(source))?;
            if witness.is_joined() || !witness.scope_identity().same_scope(&self.scope) || !self.scope.resource_open() {
                return Err(error(Stage::Prepare, Reason::StalePlan, "plugin startup caller is not the original live Scope"));
            }
            if Instant::now() >= deadline {
                return Err(error(Stage::Prepare, Reason::Timeout, "plugin start deadline elapsed"));
            }
            self.launch(witness, deadline).await?;
            tokio::select! {biased;
                _=self.scope_cancel.cancelled()=>Err(error(Stage::Prepare,Reason::Revoked,"plugin Scope cancelled")),
                result=self.shared.wait_native_exit()=>Err(io(Stage::Prepare, result.err().unwrap_or_else(|| std::io::Error::other("plugin root exited during readiness")))),
                result=tokio::time::timeout_at(deadline,self.ready(deadline))=>result.map_err(|source|error(Stage::Prepare,Reason::Timeout,"plugin start timed out").with_source(source))?,
            }
        }.await;
        if let Err(source) = outcome {
            self.shared.stop();
            let source = Arc::new(source);
            let _ = ready.send(Err(source.clone()));
            return if self.shared.cancel.is_cancelled() && source.reason == Reason::Cancelled
                || self.scope_cancel.is_cancelled()
            {
                Ok(())
            } else {
                Err(same_error(source))
            };
        }
        if ready.send(Ok(())).is_err() {
            self.shared.stop();
            return Ok(());
        }
        loop {
            tokio::select! {biased;
                _=self.scope_cancel.cancelled()=>{self.shared.stop(); return Ok(());},
                _=self.shared.cancel.cancelled()=>return Ok(()),
                result=self.shared.wait_native_exit()=>return match result {
                    Ok(status)=>Err(error(Stage::Receive,Reason::Io,"plugin child exited").with_source(ChildExited(status))),
                    Err(source)=>Err(io(Stage::Receive,source)),
                },
                _=tokio::time::sleep(Duration::from_millis(50))=>if let Some(status)=self.shared.status()? {return Err(error(Stage::Receive,Reason::Io,"plugin child exited").with_source(ChildExited(status)));},
            }
        }
    }
}
impl Owner {
    pub(super) async fn start_actor(&mut self, deadline: Instant) -> Result<(), ChannelError> {
        let (start, started) = oneshot::channel();
        let (ready, readied) = oneshot::channel();
        let actor = Actor {
            config: self.config.clone(),
            shared: self.shared.clone(),
            resources: self.resources.clone(),
            scope: self.tasks.scope_identity(),
            tasks: self.tasks.clone(),
            scope_cancel: self.tasks.cancellation(),
        };
        let (_, witness) = self
            .tasks
            .spawn_tracked(TaskClass::Critical, actor.run(deadline, started, ready))
            .map_err(|source| {
                error(
                    Stage::Prepare,
                    match source {
                        SpawnError::NotStarted => Reason::InvalidConfig,
                        SpawnError::Capacity => Reason::Capacity,
                        SpawnError::Closing => Reason::Revoked,
                        SpawnError::IdentityExhausted => Reason::Internal,
                    },
                    "plugin actor registration refused",
                )
                .with_source(source)
            })?;
        self.actor = Some(witness.clone());
        let mut attempt = StartupAttempt(self.shared.clone(), true);
        start.send(witness).map_err(|_| {
            error(
                Stage::Prepare,
                Reason::Cancelled,
                "plugin actor closed before startup",
            )
        })?;
        readied
            .await
            .map_err(|source| {
                error(
                    Stage::Prepare,
                    Reason::Internal,
                    "plugin actor ended before readiness",
                )
                .with_source(source)
            })?
            .map_err(same_error)?;
        attempt.1 = false;
        Ok(())
    }
}
#[derive(Debug)]
struct OriginalFailure(Arc<ChannelError>);
impl std::fmt::Display for OriginalFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self.0.as_ref(), f)
    }
}
impl std::error::Error for OriginalFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
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

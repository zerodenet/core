//! Ownership of one cold recipe, installed before its first start.
use super::{ComponentRecipe, ValidatedRecipe};
mod cleanup;
mod resources;
pub(super) use resources::HostResources;
#[cfg(test)]
pub(super) use resources::NoResources;
use std::sync::{Arc, OnceLock};
use tokio::{
    sync::{Mutex, OnceCell},
    time::Instant,
};
use zero_core::component::*;
use zero_platform_tokio::contract::{
    component::{BuildContext, ComponentInstance},
    lifecycle::CloseReport,
    scope::TaskScope,
    stream::DynStreamOutbound,
    CancellationToken, ChannelError, CloseReason, DeliveryState, Impact, Reason, Stage,
};

pub(crate) type SharedError = Arc<ChannelError>;
pub(crate) fn invalid(reason: Reason, message: impl Into<String>) -> ChannelError {
    let mut error = ChannelError::new(Stage::Prepare, reason, Impact::Component)
        .with_source(std::io::Error::other(message.into()));
    error.delivery = DeliveryState::NotSubmitted;
    error
}
pub(crate) fn retained(error: SharedError) -> ChannelError {
    let mut retained = ChannelError::new(error.stage, error.reason, error.impact);
    retained.component = error.component;
    retained.session = error.session;
    retained.delivery = error.delivery;
    retained.retry = error.retry;
    retained.with_source(error)
}

pub(crate) struct ComponentSlot {
    pub(crate) id: ComponentId,
    pub(crate) generation: ResourceGeneration,
    pub(crate) recipe: ComponentRecipe,
    dependencies: Arc<[ComponentInstanceId]>,
    legacy_connect_stage: &'static str,
    pub(crate) dial: zero_traits::DialPolicy,
    cancel: CancellationToken,
    live: OnceCell<Result<Live, SharedError>>,
    start_gate: Mutex<()>,
}
struct Live {
    stream: Option<Arc<dyn DynStreamOutbound>>,
    started: OnceLock<Result<(), SharedError>>,
    owner: Mutex<Owner>,
    resources: Arc<HostResources>,
}
struct Owner {
    instance: ComponentInstance,
    tasks: TaskScope,
    pending_close: Option<Box<CloseReport>>,
}

// Cancels admission when the initiating future is dropped. The actual owner
// remains installed for explicit, confirmed cleanup by the host.
struct StartCancellation {
    cancel: CancellationToken,
    armed: bool,
}
impl Drop for StartCancellation {
    fn drop(&mut self) {
        if self.armed {
            self.cancel.cancel();
        }
    }
}

impl ComponentSlot {
    pub(crate) fn new(
        id: ComponentId,
        generation: ResourceGeneration,
        recipe: ValidatedRecipe,
        dial: zero_traits::DialPolicy,
    ) -> Self {
        Self::from_recipe(
            id,
            generation,
            recipe.component_recipe(),
            dial,
            recipe.legacy_connect_stage(),
        )
    }
    pub(crate) fn from_recipe(
        id: ComponentId,
        generation: ResourceGeneration,
        recipe: ComponentRecipe,
        dial: zero_traits::DialPolicy,
        legacy_connect_stage: &'static str,
    ) -> Self {
        Self::from_bound_recipe(
            id,
            generation,
            recipe,
            dial,
            legacy_connect_stage,
            Arc::from([]),
        )
    }
    pub(crate) fn from_bound_recipe(
        id: ComponentId,
        generation: ResourceGeneration,
        recipe: ComponentRecipe,
        dial: zero_traits::DialPolicy,
        legacy_connect_stage: &'static str,
        dependencies: Arc<[ComponentInstanceId]>,
    ) -> Self {
        Self {
            id,
            generation,
            recipe,
            dependencies,
            legacy_connect_stage,
            dial,
            cancel: CancellationToken::new(),
            live: OnceCell::new(),
            start_gate: Mutex::new(()),
        }
    }
    pub(crate) fn legacy_connect_stage(&self) -> &'static str {
        self.legacy_connect_stage
    }
    pub(crate) fn instance_id(&self) -> ComponentInstanceId {
        ComponentInstanceId {
            component: self.id,
            generation: self.generation,
        }
    }
    pub(crate) fn dependency_instances(&self) -> &[ComponentInstanceId] {
        &self.dependencies
    }
    pub(crate) fn role_id(&self) -> RoleInstanceId {
        RoleInstanceId {
            component: self.id,
            role: RoleId::Outbound(OutboundId(self.id.0)),
            generation: self.generation,
        }
    }
    pub(crate) async fn stream(
        &self,
        deadline: Instant,
    ) -> Result<Arc<dyn DynStreamOutbound>, ChannelError> {
        self.start(deadline).await?;
        self.live
            .get()
            .and_then(|live| live.as_ref().ok())
            .and_then(|live| live.stream.clone())
            .ok_or_else(|| self.failure(Reason::Internal, "missing built Stream role"))
    }
    pub(crate) async fn start(&self, deadline: Instant) -> Result<(), ChannelError> {
        if self.cancel.is_cancelled() {
            return Err(self.failure(Reason::Revoked, "component was stopped"));
        }
        if Instant::now() >= deadline {
            return Err(self.failure(Reason::Timeout, "component start deadline elapsed"));
        }
        if let Some(result) = self.live.get() {
            match result {
                Ok(live) if live.started.get().is_none() => {}
                _ => return started(self.id, result),
            }
        }
        // An exhausted waiter cannot fault the initializer or acquire its I/O.
        let _starting = tokio::time::timeout_at(deadline, self.start_gate.lock())
            .await
            .map_err(|_| self.failure(Reason::Timeout, "component start deadline elapsed"))?;
        if self.cancel.is_cancelled() {
            return Err(self.failure(Reason::Revoked, "component was stopped"));
        }
        if Instant::now() >= deadline {
            return Err(self.failure(Reason::Timeout, "component start deadline elapsed"));
        }
        let live = self
            .live
            .get_or_init(|| async {
                // No await or resource acquisition inside this initializer. The
                // persistent slot owns Live before any start future can suspend.
                let runtime = tokio::runtime::Handle::current();
                let tasks = match self.recipe.spec().task_limit {
                    Some(limit) => TaskScope::new(runtime, limit),
                    None => TaskScope::empty(runtime),
                };
                let resources =
                    HostResources::new(self.id, &self.recipe.spec().resources, self.cancel.clone())
                        .map_err(Arc::new)?;
                let context = BuildContext {
                    component: self.id,
                    generation: self.generation,
                    dependencies: self.dependencies.clone(),
                    roles: ComponentRoleIds {
                        outbound: self
                            .recipe
                            .spec()
                            .roles
                            .contains(&zero_platform_tokio::contract::component::RoleKind::Stream)
                            .then_some(OutboundId(self.id.0)),
                        ..Default::default()
                    },
                    resources: resources.clone(),
                    tasks: tasks.registrar(),
                    cancel: self.cancel.child_token(),
                    background_carrier: None,
                };
                let instance = self.recipe.build(context).map_err(Arc::new)?;
                Ok(Live {
                    resources,
                    stream: instance.roles.stream.clone(),
                    started: OnceLock::new(),
                    owner: Mutex::new(Owner {
                        instance,
                        tasks,
                        pending_close: None,
                    }),
                })
            })
            .await;
        if let Ok(live) = live {
            if live.started.get().is_none() {
                let mut owner = live.owner.lock().await;
                let mut cancellation = StartCancellation {
                    cancel: self.cancel.clone(),
                    armed: true,
                };
                let started = match live.resources.activate(deadline) {
                    Ok(_access) => tokio::select! {
                    biased;
                    _ = self.cancel.cancelled() => Err(self.failure(Reason::Revoked, "component was stopped")),
                    result = tokio::time::timeout_at(deadline, owner.instance.lifecycle.start(deadline)) => result.unwrap_or_else(|_| Err(self.failure(Reason::Timeout, "component start deadline elapsed"))),
                    },
                    Err(error) => Err(error),
                };
                if started.is_err() {
                    owner.instance.lifecycle.revoke(CloseReason::Shutdown);
                }
                live.started
                    .set(started.map_err(Arc::new))
                    .expect("component start is serialized");
                cancellation.armed = false;
            }
        }
        started(self.id, live)
    }
    fn failure(&self, reason: Reason, message: &'static str) -> ChannelError {
        let mut error = invalid(reason, message);
        error.component = Some(self.id);
        error
    }
    pub(crate) async fn shutdown(&self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.revoke();
        let _starting = tokio::time::timeout_at(deadline, self.start_gate.lock())
            .await
            .map_err(|_| {
                let mut error = self.failure(Reason::Timeout, "component cleanup deadline elapsed");
                error.stage = Stage::Close;
                error
            })?;
        match self.live.get() {
            Some(Ok(live)) => {
                let mut owner = live.owner.lock().await;
                owner.shutdown(self.id, deadline).await
            }
            Some(Err(_)) | None => Ok(CloseReport::default()),
        }
    }
    pub(crate) fn revoke(&self) {
        self.cancel.cancel();
    }
}

fn started(component: ComponentId, live: &Result<Live, SharedError>) -> Result<(), ChannelError> {
    let result = match live {
        Ok(live) => match live.started.get() {
            Some(Err(error)) => Err(retained(error.clone())),
            Some(Ok(())) => Ok(()),
            None => Err(invalid(Reason::Revoked, "component start is not complete")),
        },
        Err(error) => Err(retained(error.clone())),
    };
    result.map_err(|mut error| {
        error.component.get_or_insert(component);
        error
    })
}

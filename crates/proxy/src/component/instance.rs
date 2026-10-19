//! Ownership of one cold recipe, installed before its first start.
use super::ValidatedRecipe;
use std::{path::PathBuf, sync::Arc};
use tokio::{
    sync::{Mutex, OnceCell},
    time::Instant,
};
use zero_core::{component::*, request::Destination};
use zero_platform_tokio::contract::{
    component::{BuildContext, ComponentInstance, ResourceAccess},
    lifecycle::CloseReport,
    packet::BoxPacketSession,
    scope::TaskScope,
    stream::DynStreamOutbound,
    BoxFuture, CancellationToken, ChannelError, CloseReason, DeliveryState, Impact, Reason, Stage,
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
    pub(crate) recipe: ValidatedRecipe,
    cancel: CancellationToken,
    live: OnceCell<Result<Live, SharedError>>,
    start_gate: Mutex<()>,
}
struct Live {
    stream: Option<Arc<dyn DynStreamOutbound>>,
    start_error: Option<SharedError>,
    owner: Mutex<Owner>,
}
struct Owner {
    instance: ComponentInstance,
    tasks: TaskScope,
}

impl ComponentSlot {
    pub(crate) fn new(
        id: ComponentId,
        generation: ResourceGeneration,
        recipe: ValidatedRecipe,
    ) -> Self {
        Self {
            id,
            generation,
            recipe,
            cancel: CancellationToken::new(),
            live: OnceCell::new(),
            start_gate: Mutex::new(()),
        }
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
        if self.cancel.is_cancelled() {
            return Err(invalid(Reason::Revoked, "component was stopped"));
        }
        // A caller's exhausted budget must not fault or initialize the component.
        if Instant::now() >= deadline {
            return Err(invalid(Reason::Timeout, "component start deadline elapsed"));
        }
        if let Some(live) = self.live.get() {
            return stream(live);
        }
        let _starting = self.start_gate.lock().await;
        if self.cancel.is_cancelled() {
            return Err(invalid(Reason::Revoked, "component was stopped"));
        }
        let live = self
            .live
            .get_or_init(|| async {
                let tasks = TaskScope::empty(tokio::runtime::Handle::current());
                let context = BuildContext {
                    component: self.id,
                    generation: self.generation,
                    roles: ComponentRoleIds {
                        outbound: Some(OutboundId(self.id.0)),
                        ..Default::default()
                    },
                    resources: Arc::new(NoResources),
                    tasks: tasks.registrar(),
                    cancel: self.cancel.child_token(),
                    background_carrier: None,
                };
                let instance = self.recipe.build(context).map_err(Arc::new)?;
                // Preserve the unique owner before executing even the first start step.
                let mut owner = Owner { instance, tasks };
                let started = owner.instance.lifecycle.start(deadline).await;
                let stream = owner.instance.roles.stream.clone();
                // Failed starts retain their owner until the host confirms shutdown.
                Ok(Live {
                    stream,
                    start_error: started.err().map(Arc::new),
                    owner: Mutex::new(owner),
                })
            })
            .await;
        stream(live)
    }
    pub(crate) async fn shutdown(&self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.revoke();
        let _starting = self.start_gate.lock().await;
        match self.live.get() {
            Some(Ok(live)) => {
                let mut owner = live.owner.lock().await;
                owner.instance.lifecycle.revoke(CloseReason::Shutdown);
                let mut report = owner.instance.lifecycle.shutdown(deadline).await?;
                report.append(owner.tasks.shutdown(deadline).await);
                Ok(report)
            }
            Some(Err(_)) | None => Ok(CloseReport::default()),
        }
    }
    pub(crate) fn revoke(&self) {
        self.cancel.cancel();
    }
}

fn stream(live: &Result<Live, SharedError>) -> Result<Arc<dyn DynStreamOutbound>, ChannelError> {
    match live {
        Ok(live) => match &live.start_error {
            Some(error) => Err(retained(error.clone())),
            None => live
                .stream
                .clone()
                .ok_or_else(|| invalid(Reason::Internal, "missing built Stream role")),
        },
        Err(error) => Err(retained(error.clone())),
    }
}

// This transitional slot is granted only the zero-resource Direct descriptor.
// A protocol declaring resources must use a host granting its exact declarations.
struct NoResources;
impl ResourceAccess for NoResources {
    fn read_file(&self, _: PathBuf, _: usize) -> BoxFuture<'_, Result<Vec<u8>, ChannelError>> {
        Box::pin(async { Err(invalid(Reason::Internal, "undeclared file access")) })
    }
    fn listen_tcp(
        &self,
        _: Destination,
    ) -> BoxFuture<'_, Result<tokio::net::TcpListener, ChannelError>> {
        Box::pin(async { Err(invalid(Reason::Internal, "undeclared listener")) })
    }
    fn listen_datagram(
        &self,
        _: Destination,
    ) -> BoxFuture<'_, Result<tokio::net::UdpSocket, ChannelError>> {
        Box::pin(async { Err(invalid(Reason::Internal, "undeclared datagram listener")) })
    }
    fn open_device(&self, _: Arc<str>) -> BoxFuture<'_, Result<BoxPacketSession, ChannelError>> {
        Box::pin(async { Err(invalid(Reason::Internal, "undeclared device")) })
    }
}

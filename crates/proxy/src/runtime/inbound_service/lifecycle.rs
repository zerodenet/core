//! One persistent Service owner shared by Proxy clones. Standard Source
//! lifecycles and actual logical loans remain separate from donor native close.
use super::{NativeServiceOperation, ServiceContext};
use crate::{
    protocol_registry::TcpRuntimeServices, runtime::route_runtime::SharedIngressRuntimeServices,
};
use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::{
    sync::{watch, Mutex},
    time::{timeout_at, Instant},
};
use zero_core::component::TaskId;
use zero_engine::EngineError;
use zero_platform_tokio::contract::{
    scope::{
        HostedExitReceipt, HostedScope, TaskClass, TaskJoined, TaskRegistrationBudget, TaskScope,
    },
    ChannelError,
};
mod driving;
mod replacement;
mod state;

#[derive(Clone, Default)]
pub(crate) struct InboundServices {
    owner: Arc<Mutex<State>>,
}
impl std::fmt::Debug for InboundServices {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InboundServices").finish_non_exhaustive()
    }
}
#[derive(Default)]
struct State {
    scope: Option<HostedScope>,
    cold: Option<(
        HostedScope,
        Vec<zero_platform_tokio::contract::scope::TaskLaunch>,
        NonZeroUsize,
    )>,
    retained: Arc<super::native::Retained>,
    // Retain pure accepted factory recipes, never a second transport runtime
    // or a source-materialization callback that rereads protocol material.
    previous: Vec<NativeServiceOperation>,
    staged: Option<Vec<NativeServiceOperation>>,
    suspended: bool,
    mains: BTreeMap<TaskId, Arc<AtomicBool>>,
    // Real original errors are retained before their original Host ACK.
    failures: BTreeMap<TaskId, TaskJoined>,
    ack_failure: Option<Arc<ChannelError>>,
    transition_failure: Option<Arc<ChannelError>>,
    terminal: Option<TaskJoined>,
    stopping: bool,
}
impl InboundServices {
    pub(crate) fn recovery_permitted(&self) -> bool {
        self.owner.try_lock().is_ok_and(|owner| {
            owner.transition_failure.is_none()
                && owner.failures.is_empty()
                && owner.ack_failure.is_none()
                && owner.terminal.is_none()
                && !(owner.stopping && (owner.scope.is_some() || !owner.retained.is_empty()))
        })
    }
    pub(crate) async fn begin_run(&self) -> Result<(), EngineError> {
        let mut owner = self.owner.lock().await;
        if owner.scope.is_some() || !owner.retained.is_empty() {
            return Err(
                std::io::Error::other("previous Service Scope still requires retirement").into(),
            );
        }
        owner.scope_failure()?;
        // A completed retry may clear a prior operation timeout. Actual failed
        // task/ACK receipts stay sticky and were checked independently above.
        owner.transition_failure.take();
        owner.stopping = false;
        Ok(())
    }
    pub(crate) async fn shutdown(&mut self) -> Result<(), EngineError> {
        let mut owner = self.owner.lock().await;
        owner.cold.take();
        owner
            .close(Instant::now() + std::time::Duration::from_secs(5))
            .await
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.owner.try_lock().is_ok_and(|owner| {
            owner.scope.is_none()
                && owner.retained.is_empty()
                && owner.failures.is_empty()
                && owner.ack_failure.is_none()
                && owner.transition_failure.is_none()
                && owner.terminal.is_none()
        })
    }
    pub(crate) async fn join_next(
        &mut self,
    ) -> Option<Result<Result<(), EngineError>, tokio::task::JoinError>> {
        let mut owner = self.owner.lock().await;
        loop {
            if let Err(source) = owner.retained_failure() {
                return Some(Ok(Err(source)));
            }
            let scope = owner.scope.as_mut()?;
            let receipt = scope.next_receipt().await?;
            let result = owner.receive(receipt);
            if result.is_err() {
                return Some(Ok(result));
            }
            if let Err(source) = owner.retained_failure() {
                return Some(Ok(Err(source)));
            }
        }
    }
    pub(crate) async fn replace(
        &mut self,
        prepared: Vec<NativeServiceOperation>,
        services: TcpRuntimeServices,
        shutdown: watch::Receiver<bool>,
    ) -> Result<(), EngineError> {
        let mut owner = self.owner.lock().await;
        owner.retained_failure()?;
        if prepared.is_empty() || *shutdown.borrow() {
            if prepared.is_empty() {
                owner.staged = Some(Vec::new());
            }
            return match owner
                .close(Instant::now() + std::time::Duration::from_secs(5))
                .await
            {
                Ok(()) => Ok(()),
                Err(source) => Err(owner.fail_transition(source)),
            };
        }
        let limit = NonZeroUsize::new(
            services
                .config()
                .runtime
                .effective_component_total_max_tasks(),
        )
        .ok_or_else(|| std::io::Error::other("Service task ceiling is zero"))?;
        std::future::poll_fn(|cx| {
            for _ in 0..64 {
                let Some(scope) = owner.scope.as_mut() else {
                    return std::task::Poll::Ready(owner.retained_failure());
                };
                match scope.poll_exit(cx) {
                    std::task::Poll::Ready(Some(receipt)) => {
                        let _ = owner.receive(receipt);
                    }
                    std::task::Poll::Ready(None) | std::task::Poll::Pending => {
                        return std::task::Poll::Ready(owner.retained_failure())
                    }
                }
            }
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        })
        .await?;
        // Reserve only cold task grants before stopping old work. A normal
        // quota refusal must leave the original Service running untouched.
        let (candidate, launches) = match owner.cold.take() {
            Some((scope, launches, planned))
                if planned == limit && launches.len() == prepared.len() =>
            {
                (scope, launches)
            }
            Some(_) => {
                return Err(std::io::Error::other(
                    "active Source cold Main reservation differs from its candidate",
                )
                .into())
            }
            None => State::prepare(limit, services.service_task_budget(), prepared.len())?,
        };
        if let Err(source) = owner
            .close(Instant::now() + std::time::Duration::from_secs(5))
            .await
        {
            return Err(owner.fail_transition(source));
        }
        if let Err(source) = owner.activate(candidate) {
            return Err(owner.fail_transition(source));
        }
        let recipes = prepared.clone();
        // The actual Scope is persistent before any registered future can run.
        let registrar = owner.scope.as_ref().unwrap().registrar();
        for (operation, launch) in prepared.into_iter().zip(launches) {
            let native = operation.install(&services, registrar.clone(), &owner.retained)?;
            owner
                .retained
                .start(Instant::now() + std::time::Duration::from_secs(5))
                .await?;
            let cancel = registrar.cancellation();
            let context = ServiceContext {
                native,
                retained: owner.retained.clone(),
                max_tasks: NonZeroUsize::new(
                    services
                        .config()
                        .runtime
                        .effective_inbound_max_tasks_per_carrier(520),
                )
                .ok_or_else(|| std::io::Error::other("active carrier task ceiling is zero"))?,
                max_resources: services
                    .config()
                    .runtime
                    .effective_inbound_max_resources_per_carrier(257),
                ingress: SharedIngressRuntimeServices::new(services.clone()),
                tasks: registrar.downgrade(),
                cancel: cancel.clone(),
            };
            let mut shutdown = shutdown.clone();
            let requested = Arc::new(AtomicBool::new(false));
            let stop_marker = requested.clone();
            let launched = launch.spawn_tracked(async move {
                if *shutdown.borrow_and_update() {
                    stop_marker.store(true,Ordering::Release);
                    return Ok(());
                }
                let run = operation.run(context);
                tokio::pin!(run);
                // A stop wakes the real operation, which drains/returns through
                // its same Scope. Never drop its pending IO as an exit receipt.
                tokio::select! { biased;
                    result=&mut run=>{
                        if cancel.is_cancelled() || *shutdown.borrow() { stop_marker.store(true,Ordering::Release); }
                        return result.map_err(failure);
                    },
                    _=shutdown.changed()=>{stop_marker.store(true,Ordering::Release);cancel.cancel();},
                }
                run.await.map_err(failure)
            });
            match launched {
                Ok((task, _)) => {
                    owner.mains.insert(task, requested);
                }
                Err(source) => {
                    owner.scope.as_mut().unwrap().begin_shutdown();
                    return Err(owner.fail_transition(std::io::Error::other(source).into()));
                }
            }
        }
        owner.staged = Some(recipes);
        Ok(())
    }
}
pub(super) fn failure(source: EngineError) -> ChannelError {
    ChannelError::new(
        zero_platform_tokio::contract::Stage::Close,
        zero_platform_tokio::contract::Reason::Io,
        zero_platform_tokio::contract::Impact::Component,
    )
    .with_source(source)
}

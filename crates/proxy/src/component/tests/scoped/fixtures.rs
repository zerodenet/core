use crate::component::{instance::ComponentSlot, ComponentRegistry};
use std::{
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
};
use tokio::{
    io::DuplexStream,
    sync::{oneshot, Notify},
    time::{timeout_at, Instant},
};
use zero_core::component::{ComponentId, ResourceGeneration, TaskId};
use zero_platform_tokio::contract::{
    component::{
        BuildContext, ComponentFactory, ComponentInstance, ComponentRoles, ComponentSpec,
        ResourceOwner,
    },
    lifecycle::{
        erase_lifecycle, BuildError, CloseReport, ResourceLifecycle, TaskClass, Unfinished,
    },
    scope::TaskRegistrar,
    BoxFuture, ChannelError, CloseReason, Impact, Reason, Stage,
};

#[derive(Default)]
pub(super) struct State {
    pub(super) started: AtomicUsize,
    pub(super) closed: AtomicBool,
    pub(super) peer: Mutex<Option<DuplexStream>>,
    pub(super) registrar: Mutex<Option<TaskRegistrar>>,
    pub(super) task: Mutex<Option<TaskId>>,
    pub(super) release: Notify,
}
struct Factory {
    state: Arc<State>,
    stall: bool,
    fail_close: bool,
    fail_task: bool,
}
struct Owner {
    state: Arc<State>,
    io: Arc<Mutex<Option<DuplexStream>>>,
    registrar: TaskRegistrar,
    driver: Option<BoxFuture<'static, Result<(), ChannelError>>>,
    finish: Option<oneshot::Sender<()>>,
    done: oneshot::Receiver<()>,
    registered: bool,
    fail_close: bool,
}
fn failure(message: &'static str) -> ChannelError {
    let mut error = ChannelError::new(Stage::Close, Reason::Io, Impact::Component)
        .with_source(std::io::Error::other(message));
    error.component = Some(ComponentId(9));
    error
}
impl ResourceLifecycle for Owner {
    async fn start(&mut self, _: Instant) -> Result<(), BuildError> {
        let driver = self.driver.take().expect("one declared driver");
        let task = self
            .registrar
            .spawn(TaskClass::Critical, driver)
            .map_err(|_| failure("driver registration failed"))?;
        self.registered = true;
        *self.state.task.lock().unwrap() = Some(task);
        self.state.started.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {}
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, BuildError> {
        if !self.registered {
            drop(self.driver.take());
            drop(self.io.lock().unwrap().take());
            self.state.closed.store(true, Ordering::SeqCst);
        }
        if let Some(finish) = self.finish.take() {
            let _ = finish.send(());
        }
        if !self.state.closed.load(Ordering::SeqCst) {
            match timeout_at(deadline, &mut self.done).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) => return Err(failure("driver ended without closing its I/O")),
                Err(_) => {
                    return Ok(CloseReport {
                        unfinished: vec![Unfinished::Io("owned driver stream".into())],
                        ..Default::default()
                    })
                }
            }
        }
        if self.fail_close {
            self.fail_close = false;
            return Err(failure("protocol cleanup confirmation failed once"));
        }
        Ok(CloseReport {
            closed_io: vec!["owned driver stream".into()],
            ..Default::default()
        })
    }
}
impl ComponentFactory for Factory {
    type Options = ();
    fn describe(&self, _: &()) -> ComponentSpec {
        ComponentSpec {
            owner: ResourceOwner::Component,
            roles: vec![],
            dependencies: vec![],
            static_identity: Arc::from([]),
            resources: vec![],
            task_limit: NonZeroUsize::new(1),
        }
    }
    fn build(&self, _: Arc<()>, context: BuildContext) -> Result<ComponentInstance, BuildError> {
        assert!(context.roles.inbound.is_none());
        assert!(context.roles.outbound.is_none());
        assert!(context.roles.endpoint.is_none());
        let (io, peer) = tokio::io::duplex(8);
        let io = Arc::new(Mutex::new(Some(io)));
        *self.state.peer.lock().unwrap() = Some(peer);
        *self.state.registrar.lock().unwrap() = Some(context.tasks.clone());
        let (finish, finish_rx) = oneshot::channel();
        let (done_tx, done) = oneshot::channel();
        let state = self.state.clone();
        let task_io = io.clone();
        let cancel = context.tasks.cancellation();
        let (stall, fail_task) = (self.stall, self.fail_task);
        let driver = Box::pin(async move {
            cancel.cancelled().await;
            finish_rx
                .await
                .map_err(|_| failure("protocol hook was not called"))?;
            if stall {
                state.release.notified().await;
            }
            drop(task_io.lock().unwrap().take());
            state.closed.store(true, Ordering::SeqCst);
            let _ = done_tx.send(());
            if fail_task {
                Err(failure("scoped driver reported a terminal failure"))
            } else {
                Ok(())
            }
        });
        Ok(ComponentInstance {
            lifecycle: erase_lifecycle(Owner {
                state: self.state.clone(),
                io,
                registrar: context.tasks,
                driver: Some(driver),
                finish: Some(finish),
                done,
                registered: false,
                fail_close: self.fail_close,
            }),
            roles: ComponentRoles::default(),
        })
    }
}
pub(super) fn slot(
    stall: bool,
    fail_close: bool,
    fail_task: bool,
) -> (Arc<ComponentSlot>, Arc<State>) {
    use zero_traits::ProtocolMetadata;
    let state = Arc::new(State::default());
    let mut registry = ComponentRegistry::<()>::default();
    registry
        .register(
            crate::adapters::direct::DirectAdapter::default().descriptor(),
            Factory {
                state: state.clone(),
                stall,
                fail_close,
                fail_task,
            },
            |_| Ok(()),
        )
        .unwrap();
    let slot = Arc::new(ComponentSlot::new(
        ComponentId(9),
        ResourceGeneration(2),
        registry.prepare("direct", &()).unwrap(),
        Default::default(),
    ));
    (slot, state)
}

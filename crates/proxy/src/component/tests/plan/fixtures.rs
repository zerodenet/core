use super::super::*;
use std::num::NonZeroUsize;
use tokio::{
    io::{AsyncReadExt, DuplexStream},
    sync::Notify,
    time::Instant,
};
use zero_core::component::TaskId;
use zero_platform_tokio::contract::{
    component::{
        BuildContext, ComponentFactory, ComponentInstance, ComponentRoles, ComponentSpec,
        ResourceOwner,
    },
    lifecycle::{erase_lifecycle, BuildError, CloseReport, ResourceLifecycle, TaskClass},
    scope::TaskRegistrar,
    CloseReason,
};
use zero_traits::ProtocolMetadata;

#[derive(Default)]
pub(super) struct State {
    pub(super) peer: Mutex<Option<DuplexStream>>,
    pub(super) task: Mutex<Option<TaskId>>,
    pub(super) native_id: Mutex<Option<tokio::task::Id>>,
    pub(super) pending: Notify,
    pub(super) closed: AtomicBool,
}
struct Factory {
    state: Arc<State>,
    panic: bool,
}
struct Owner {
    state: Arc<State>,
    io: Option<DuplexStream>,
    registrar: TaskRegistrar,
    panic: bool,
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
            task_limit: self.panic.then(|| NonZeroUsize::new(1).unwrap()),
        }
    }
    fn build(&self, _: Arc<()>, context: BuildContext) -> Result<ComponentInstance, BuildError> {
        let (io, peer) = tokio::io::duplex(8);
        *self.state.peer.lock().unwrap() = Some(peer);
        Ok(ComponentInstance {
            lifecycle: erase_lifecycle(Owner {
                state: self.state.clone(),
                io: Some(io),
                registrar: context.tasks,
                panic: self.panic,
            }),
            roles: ComponentRoles::default(),
        })
    }
}
impl ResourceLifecycle for Owner {
    async fn start(&mut self, _: Instant) -> Result<(), BuildError> {
        if self.panic {
            let mut io = self.io.take().unwrap();
            let state = self.state.clone();
            let id = self
                .registrar
                .spawn(TaskClass::Critical, async move {
                    *state.native_id.lock().unwrap() = Some(tokio::task::id());
                    io.read_u8().await.unwrap();
                    panic!("actual critical component driver panic");
                })
                .unwrap();
            *self.state.task.lock().unwrap() = Some(id);
        }
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {}
    async fn shutdown(&mut self, _: Instant) -> Result<CloseReport, BuildError> {
        if let Some(io) = &mut self.io {
            let result = {
                let read = io.read_u8();
                tokio::pin!(read);
                std::future::poll_fn(|cx| {
                    let result = std::future::Future::poll(read.as_mut(), cx);
                    if result.is_pending() {
                        self.state.pending.notify_one();
                    }
                    result
                })
                .await
            };
            assert_eq!(
                result.unwrap_err().kind(),
                std::io::ErrorKind::UnexpectedEof
            );
            drop(self.io.take());
            self.state.closed.store(true, Ordering::Release);
            return Ok(CloseReport {
                closed_io: vec!["actual cleanup stream".into()],
                ..Default::default()
            });
        }
        Ok(CloseReport::default())
    }
}
pub(super) fn slot(id: u64, panic: bool) -> (Arc<ComponentSlot>, Arc<State>) {
    let state = Arc::new(State::default());
    let mut registry = ComponentRegistry::<()>::default();
    registry
        .register(
            crate::adapters::direct::DirectAdapter::default().descriptor(),
            Factory {
                state: state.clone(),
                panic,
            },
            |_| Ok(()),
        )
        .unwrap();
    (
        Arc::new(ComponentSlot::new(
            ComponentId(id),
            ResourceGeneration(1),
            registry.prepare("direct", &()).unwrap(),
            Default::default(),
        )),
        state,
    )
}

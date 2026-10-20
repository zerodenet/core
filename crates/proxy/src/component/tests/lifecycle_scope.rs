//! Lock-boundary qualification only. These tests are the explicit join receiver;
//! live catalog supervision and automatic Critical-fault delivery are separate.
use super::*;
use std::{
    future::{poll_fn, Future},
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex as StdMutex,
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, DuplexStream},
    sync::Notify,
    time::{timeout, Duration},
};
use zero_platform_tokio::contract::{
    component::{ComponentFactory, ComponentRoles, ComponentSpec, ResourceOwner},
    lifecycle::{erase_lifecycle, BuildError, ResourceLifecycle},
    scope::{TaskClass, TaskJoinWitness, TaskRegistrar},
};

#[derive(Default)]
struct State {
    start_peer: StdMutex<Option<DuplexStream>>,
    task_peer: StdMutex<Option<DuplexStream>>,
    start_pending: Notify,
    task_pending: Notify,
    native: StdMutex<Option<tokio::task::Id>>,
    witness: StdMutex<Option<TaskJoinWitness>>,
    shutdowns: AtomicUsize,
}
struct Factory(Arc<State>);
struct Lifecycle {
    state: Arc<State>,
    registrar: TaskRegistrar,
    start: Option<DuplexStream>,
    driver: Option<DuplexStream>,
}
impl ComponentFactory for Factory {
    type Options = ();
    fn describe(&self, _: &()) -> ComponentSpec {
        ComponentSpec {
            owner: ResourceOwner::Component,
            roles: vec![],
            dependencies: vec![],
            static_identity: Arc::from([]),
            resources: vec![], // Only in-memory streams and one actual task.
            task_limit: NonZeroUsize::new(1),
        }
    }
    fn build(&self, _: Arc<()>, context: BuildContext) -> Result<ComponentInstance, BuildError> {
        let (start, start_peer) = tokio::io::duplex(1);
        let (driver, task_peer) = tokio::io::duplex(1);
        *self.0.start_peer.lock().unwrap() = Some(start_peer);
        *self.0.task_peer.lock().unwrap() = Some(task_peer);
        Ok(ComponentInstance {
            roles: ComponentRoles::default(),
            lifecycle: erase_lifecycle(Lifecycle {
                state: self.0.clone(),
                registrar: context.tasks,
                start: Some(start),
                driver: Some(driver),
            }),
        })
    }
}
async fn read_pending(io: &mut DuplexStream, pending: &Notify) -> std::io::Result<u8> {
    let reading = io.read_u8();
    tokio::pin!(reading);
    poll_fn(|cx| {
        let result = reading.as_mut().poll(cx);
        if result.is_pending() {
            pending.notify_one();
        }
        result
    })
    .await
}
impl ResourceLifecycle for Lifecycle {
    async fn start(&mut self, _: Instant) -> Result<(), BuildError> {
        let mut driver = self.driver.take().unwrap();
        let state = self.state.clone();
        let stop = self.registrar.cancellation();
        let (_, witness) = self
            .registrar
            .spawn_tracked(TaskClass::Critical, async move {
                *state.native.lock().unwrap() = Some(tokio::task::id());
                tokio::select! {
                    _ = stop.cancelled() => Ok(()),
                    result = read_pending(&mut driver, &state.task_pending) => {
                        result.unwrap();
                        panic!("actual Critical task while lifecycle start reads");
                    }
                }
            })
            .unwrap();
        *self.state.witness.lock().unwrap() = Some(witness);
        read_pending(self.start.as_mut().unwrap(), &self.state.start_pending)
            .await
            .unwrap();
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {}
    async fn shutdown(&mut self, _: Instant) -> Result<CloseReport, BuildError> {
        self.state.shutdowns.fetch_add(1, Ordering::SeqCst);
        drop(self.start.take());
        drop(self.driver.take());
        // No native descriptors, transferred users or native-close ACKs here.
        Ok(CloseReport::default())
    }
}
fn fixture() -> (ComponentSlot, Arc<State>) {
    let state = Arc::new(State::default());
    (
        ComponentSlot::from_recipe(
            ComponentId(63),
            ResourceGeneration(2),
            ComponentRecipe::from_validated(Arc::new(Factory(state.clone())), Arc::new(())),
            Default::default(),
            "test",
        ),
        state,
    )
}

#[tokio::test]
async fn actual_scope_join_remains_accessible_while_the_same_lifecycle_start_is_pending_in_io() {
    use std::error::Error;
    timeout(Duration::from_secs(3), async {
        let (slot, state) = fixture();
        let mut starting = Box::pin(slot.start(Instant::now() + Duration::from_secs(30)));
        tokio::select! {
            result = starting.as_mut() => panic!("start was not suspended: {result:?}"),
            _ = state.start_pending.notified() => {},
        }
        state.task_pending.notified().await;
        let mut peer = state.task_peer.lock().unwrap().take().unwrap();
        peer.write_u8(1).await.unwrap();
        let live = slot.live.get().unwrap().as_ref().unwrap();
        // The lifecycle is still borrowed by the real start future. This is
        // the same installed scope; no new owner, task or join registry exists.
        assert!(live.owner.try_lock().is_err());
        let exit = live.tasks.lock().await.next_exit().await.unwrap();
        let source = exit.result.unwrap_err();
        let witness = state.witness.lock().unwrap().clone().unwrap();
        let joined = witness.wait().await;
        assert_eq!(joined.id(), exit.id);
        assert_eq!(joined.class(), TaskClass::Critical);
        let original = joined.result().unwrap_err();
        let native = original
            .source()
            .unwrap()
            .downcast_ref::<tokio::task::JoinError>()
            .unwrap();
        assert!(native.is_panic());
        assert_eq!(native.id(), state.native.lock().unwrap().unwrap());
        let forwarded = source
            .source()
            .unwrap()
            .source()
            .unwrap()
            .downcast_ref::<ChannelError>()
            .unwrap();
        assert!(std::ptr::eq(forwarded, original.as_ref()));
        drop(starting); // Cancels only the borrow; persistent owners remain.
        let report = slot
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(report.is_complete());
        assert!(
            report.exited_tasks.is_empty(),
            "this test already received the actual exit"
        );
        assert_eq!(state.shutdowns.load(Ordering::SeqCst), 1);
        let mut peer = state.start_peer.lock().unwrap().take().unwrap();
        assert_eq!(
            peer.read_u8().await.unwrap_err().kind(),
            std::io::ErrorKind::UnexpectedEof
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn independent_scope_cleanup_joins_before_start_borrow_is_cancelled_without_claiming_protocol_close(
) {
    timeout(Duration::from_secs(3), async {
        let (slot, state) = fixture();
        let mut starting = Box::pin(slot.start(Instant::now() + Duration::from_secs(30)));
        tokio::select! {
            result = starting.as_mut() => panic!("start was not suspended: {result:?}"),
            _ = state.start_pending.notified() => {},
        }
        state.task_pending.notified().await;
        let live = slot.live.get().unwrap().as_ref().unwrap();
        let report = live
            .tasks
            .lock()
            .await
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await;
        assert!(report.is_complete());
        assert_eq!(report.exited_tasks.len(), 1);
        assert!(report.failed_tasks.is_empty());
        assert_eq!(
            state.shutdowns.load(Ordering::SeqCst),
            0,
            "joined scope is not protocol cleanup"
        );
        assert!(live.owner.try_lock().is_err());
        drop(starting);
        assert!(slot
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap()
            .is_complete());
        assert_eq!(state.shutdowns.load(Ordering::SeqCst), 1);
    })
    .await
    .unwrap();
}

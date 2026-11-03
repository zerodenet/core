//! Same actual hosted consumer during pending lifecycle start and cleanup.
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
struct Factory(Arc<State>, TaskClass, bool);
struct Lifecycle {
    state: Arc<State>,
    registrar: TaskRegistrar,
    start: Option<DuplexStream>,
    driver: Option<DuplexStream>,
    class: TaskClass,
    wait_start: bool,
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
                class: self.1,
                wait_start: self.2,
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
            .spawn_tracked(self.class, async move {
                *state.native.lock().unwrap() = Some(tokio::task::id());
                tokio::select! {
                    _ = stop.cancelled() => Ok(()),
                    result = read_pending(&mut driver, &state.task_pending) => {
                        result.unwrap();
                        panic!("actual component task suspended in a stream read");
                    }
                }
            })
            .unwrap();
        *self.state.witness.lock().unwrap() = Some(witness);
        if self.wait_start {
            read_pending(self.start.as_mut().unwrap(), &self.state.start_pending)
                .await
                .unwrap();
        }
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
    fixture_with(TaskClass::Critical, true)
}
fn fixture_with(class: TaskClass, wait_start: bool) -> (ComponentSlot, Arc<State>) {
    let state = Arc::new(State::default());
    (
        ComponentSlot::from_recipe(
            ComponentId(63),
            ResourceGeneration(2),
            ComponentRecipe::from_validated(
                Arc::new(Factory(state.clone(), class, wait_start)),
                Arc::new(()),
            ),
            Default::default(),
            "test",
        ),
        state,
    )
}

#[tokio::test]
async fn actual_critical_join_rejects_pending_lifecycle_start_with_the_same_original_fault() {
    use crate::component::catalog::supervision::testing::TestContext;
    use std::error::Error;
    timeout(Duration::from_secs(3), async {
        let (slot, state) = fixture();
        let host = TestContext::new([&slot]);
        let mut starting =
            Box::pin(host.drive(slot.start(Instant::now() + Duration::from_secs(30))));
        tokio::select! {
            result = starting.as_mut() => panic!("start was not suspended: {result:?}"),
            _ = state.start_pending.notified() => {},
        }
        state.task_pending.notified().await;
        let live = slot.live.get().unwrap().as_ref().unwrap();
        assert!(live.owner.try_lock().is_err());
        assert_eq!(state.shutdowns.load(Ordering::SeqCst), 0);
        let mut peer = state.task_peer.lock().unwrap().take().unwrap();
        peer.write_u8(1).await.unwrap();
        let error = starting.await.unwrap_err();
        let fault = slot.task_fault().unwrap();
        assert!(Arc::ptr_eq(
            error
                .source()
                .unwrap()
                .downcast_ref::<Arc<ComponentTaskFault>>()
                .unwrap(),
            &fault
        ));
        let witness = state.witness.lock().unwrap().clone().unwrap();
        let joined = witness.wait().await;
        assert_eq!(joined.id(), fault.id);
        assert_eq!(joined.class(), TaskClass::Critical);
        let original = joined.result().unwrap_err();
        assert!(Arc::ptr_eq(&original, &fault.source));
        let native = original
            .source()
            .unwrap()
            .downcast_ref::<tokio::task::JoinError>()
            .unwrap();
        assert!(native.is_panic());
        assert_eq!(native.id(), state.native.lock().unwrap().unwrap());
        assert!(slot.cancel.is_cancelled());
        assert_eq!(
            state.shutdowns.load(Ordering::SeqCst),
            0,
            "native task join is not lifecycle shutdown"
        );
        let report = host
            .drive(slot.shutdown(Instant::now() + Duration::from_secs(1)))
            .await
            .unwrap();
        assert!(report.is_complete());
        assert_eq!(report.exited_tasks, [joined.id()]);
        assert_eq!(report.failed_tasks.len(), 1);
        assert_eq!(report.failed_tasks[0].id, joined.id());
        assert_eq!(report.failed_tasks[0].class, TaskClass::Critical);
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
async fn hosted_cleanup_joins_while_start_is_pending_without_claiming_protocol_close() {
    use crate::component::catalog::supervision::testing::TestContext;
    timeout(Duration::from_secs(3), async {
        let (slot, state) = fixture();
        let host = TestContext::new([&slot]);
        let mut starting = Box::pin(host.drive(slot.start(Instant::now() + Duration::from_secs(30))));
        tokio::select! {
            result = starting.as_mut() => panic!("start was not suspended: {result:?}"),
            _ = state.start_pending.notified() => {},
        }
        state.task_pending.notified().await;
        let live = slot.live.get().unwrap().as_ref().unwrap();
        let mut report = CloseReport::default();
        // This explicit fixture receiver persists actual exits, while the
        // lifecycle's original borrowed start future still owns its read.
        tokio::select! {
            result = starting.as_mut() => panic!("pending lifecycle unexpectedly finished: {result:?}"),
            _ = host.drive(live.tasks.collect(Instant::now() + Duration::from_secs(1), |out| report.append(out))) => {},
        }
        assert!(report.is_complete());
        assert_eq!(report.exited_tasks.len(), 1);
        assert!(report.failed_tasks.is_empty());
        assert_eq!(state.shutdowns.load(Ordering::SeqCst), 0);
        assert!(live.owner.try_lock().is_err());
        drop(starting);
        assert!(host.drive(slot.shutdown(Instant::now() + Duration::from_secs(1))).await.unwrap().is_complete());
        assert_eq!(state.shutdowns.load(Ordering::SeqCst), 1);
    }).await.unwrap();
}

#[path = "lifecycle_scope/hosted.rs"]
mod hosted;

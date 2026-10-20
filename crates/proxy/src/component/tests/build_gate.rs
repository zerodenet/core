use super::super::{instance::ComponentSlot, ComponentRecipe};
use crate::component::catalog::supervision::testing::TestContext;
use std::{
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};
use tokio::{
    sync::Notify,
    time::{Duration, Instant},
};
use zero_core::component::{ComponentId, ResourceGeneration, TaskId};
use zero_platform_tokio::contract::{
    component::{
        BuildContext, ComponentFactory, ComponentInstance, ComponentRoles, ComponentSpec,
        ResourceOwner,
    },
    lifecycle::{erase_lifecycle, BuildError, CloseReport, ResourceLifecycle},
    scope::{SpawnError, TaskClass, TaskRegistrar},
    CloseReason,
};
#[derive(Default)]
struct State {
    build_registration: Mutex<Option<SpawnError>>,
    rejected_polled: AtomicBool,
    actual_entered: Notify,
    actual_task: Mutex<Option<TaskId>>,
    registrar: Mutex<Option<TaskRegistrar>>,
}
struct Factory(Arc<State>);
struct Owner {
    state: Arc<State>,
    registrar: TaskRegistrar,
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
        let state = self.0.clone();
        *self.0.build_registration.lock().unwrap() = Some(
            context
                .tasks
                .spawn(TaskClass::Critical, async move {
                    state.rejected_polled.store(true, Ordering::SeqCst);
                    Ok(())
                })
                .unwrap_err(),
        );
        *self.0.registrar.lock().unwrap() = Some(context.tasks.clone());
        Ok(ComponentInstance {
            lifecycle: erase_lifecycle(Owner {
                state: self.0.clone(),
                registrar: context.tasks,
            }),
            roles: ComponentRoles::default(),
        })
    }
}
impl ResourceLifecycle for Owner {
    async fn start(&mut self, _: Instant) -> Result<(), BuildError> {
        let state = self.state.clone();
        let cancel = self.registrar.cancellation();
        let task = self
            .registrar
            .spawn(TaskClass::Critical, async move {
                state.actual_entered.notify_one();
                cancel.cancelled().await;
                Ok(())
            })
            .expect("only the installed host owner can enter start registration");
        *self.state.actual_task.lock().unwrap() = Some(task);
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {}
    async fn shutdown(&mut self, _: Instant) -> Result<CloseReport, BuildError> {
        Ok(CloseReport::default())
    }
}
#[tokio::test]
async fn actual_slot_denies_build_tasks_then_registers_and_joins_the_real_start_task() {
    let state = Arc::new(State::default());
    let slot = ComponentSlot::from_recipe(
        ComponentId(7),
        ResourceGeneration(1),
        ComponentRecipe::from_validated(Arc::new(Factory(state.clone())), Arc::new(())),
        Default::default(),
        "test",
    );
    let deadline = Instant::now() + Duration::from_secs(2);
    let host = TestContext::new([&slot]);
    host.drive(slot.start(deadline)).await.unwrap();
    state.actual_entered.notified().await;
    assert_eq!(
        *state.build_registration.lock().unwrap(),
        Some(SpawnError::NotStarted)
    );
    assert!(!state.rejected_polled.load(Ordering::SeqCst));
    let task = state.actual_task.lock().unwrap().unwrap();
    let report = host.drive(slot.shutdown(deadline)).await.unwrap();
    assert!(report.is_complete());
    assert_eq!(report.exited_tasks, vec![task]);
    assert!(report.failed_tasks.is_empty());
    assert_eq!(
        state
            .registrar
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .spawn(TaskClass::Critical, async { Ok(()) }),
        Err(SpawnError::Closing)
    );
}

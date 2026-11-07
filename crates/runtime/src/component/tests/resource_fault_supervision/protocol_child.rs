//! A real Scope actor needs the actual returned child fault accepted before exit.
use super::*;
use std::sync::OnceLock;
use zero_platform_tokio::contract::{
    lifecycle::{ResourceFailure, ResourceFailurePhase, TaskClass},
    scope::{TaskJoinWitness, TaskRegistrar},
    CleanupFailureReceipt,
};
#[derive(Default)]
struct State {
    child: OnceLock<Arc<ResourceFailure>>,
    entered: Arc<tokio::sync::Notify>,
    witness: StdMutex<Option<TaskJoinWitness>>,
    grant: Arc<StdMutex<Option<JournalGrant>>>,
}
struct ChildFactory {
    path: PathBuf,
    state: Arc<State>,
}
struct ChildProtocol {
    inner: Box<dyn zero_platform_tokio::contract::lifecycle::DynResourceLifecycle>,
    actual: zero_core::component::ComponentInstanceId,
    tasks: TaskRegistrar,
    state: Arc<State>,
    path: PathBuf,
}
impl ComponentFactory for ChildFactory {
    type Options = ();
    fn describe(&self, _: &()) -> ComponentSpec {
        let mut spec = Factory(self.path.clone(), self.state.grant.clone()).describe(&());
        spec.task_limit = NonZeroUsize::new(1);
        spec
    }
    fn build(
        &self,
        options: Arc<()>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        let actual = context.instance();
        let tasks = context.tasks.clone();
        let built = Factory(self.path.clone(), self.state.grant.clone()).build(options, context)?;
        Ok(ComponentInstance {
            roles: built.roles,
            lifecycle: erase_lifecycle(ChildProtocol {
                inner: built.lifecycle,
                actual,
                tasks,
                state: self.state.clone(),
                path: self.path.clone(),
            }),
        })
    }
}
impl ResourceLifecycle for ChildProtocol {
    async fn start(&mut self, end: Instant) -> Result<(), BuildError> {
        self.inner.start(end).await?;
        let source =
            Arc::new(std::fs::File::open(self.path.with_extension("missing")).unwrap_err());
        let receipt = CleanupFailureReceipt::new();
        assert!(receipt.bind_payload(source.clone()));
        let child = Arc::new(ResourceFailure::new(
            self.actual,
            ResourceFailurePhase::ProtocolLifecycle,
            Arc::new(
                ChannelError::new(Stage::Close, Reason::Io, Impact::Component)
                    .with_source(source)
                    .with_cleanup_failure_receipt(receipt.clone()),
            ),
        ));
        self.state.child.set(child).unwrap();
        let entered = self.state.entered.clone();
        let (_, witness) = self
            .tasks
            .spawn_tracked(TaskClass::Critical, async move {
                entered.notify_one();
                while !receipt.is_acknowledged() {
                    tokio::task::yield_now().await;
                }
                Ok(())
            })
            .unwrap();
        *self.state.witness.lock().unwrap() = Some(witness);
        self.state.entered.notified().await;
        Ok(())
    }
    fn revoke(&self, reason: CloseReason) {
        self.inner.revoke(reason);
    }
    async fn shutdown(&mut self, end: Instant) -> Result<CloseReport, BuildError> {
        let mut report = self.inner.shutdown(end).await?;
        report
            .resource_failures
            .push(self.state.child.get().unwrap().clone());
        Ok(report)
    }
}
#[tokio::test]
async fn actual_protocol_child_is_received_before_the_same_actor_join_and_keeps_its_source_after_retirement(
) {
    use crate::component::catalog::supervision::testing::TestContext;
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        let directory = tempfile::tempdir().unwrap();
        let state = Arc::new(State::default());
        let slot = ComponentSlot::from_recipe(
            ComponentId(794),
            ResourceGeneration(2),
            ComponentRecipe::from_validated(
                Arc::new(ChildFactory {
                    path: directory.path().join("journal"),
                    state: state.clone(),
                }),
                Arc::new(()),
            ),
            Default::default(),
            "protocol-child",
        );
        let host = TestContext::new([&slot]);
        let end = Instant::now() + std::time::Duration::from_secs(3);
        host.drive(slot.start(end)).await.unwrap();
        let child = state.child.get().unwrap().clone();
        let receipt = child.source.cleanup_failure_receipt().unwrap();
        assert!(!receipt.is_acknowledged());
        assert!(!child.ownership_received());
        let witness = state.witness.lock().unwrap().as_ref().unwrap().clone();
        assert!(witness.try_joined().is_none());
        let grant = state.grant.lock().unwrap().as_ref().unwrap().clone();
        assert!(!grant.native_closed());
        let report = host.drive(slot.shutdown(end)).await.unwrap();
        assert!(
            report.is_complete(),
            "same original complete report: {report:?}"
        );
        assert!(receipt.is_acknowledged() && child.ownership_received());
        assert_eq!(report.resource_failures.len(), 1);
        let canonical = report.resource_failures[0].clone();
        assert!(!Arc::ptr_eq(&canonical, &child));
        assert_eq!(canonical.instance, child.instance);
        assert_eq!(
            canonical.controller,
            ResourceFailurePhase::ProtocolLifecycle
        );
        let joined = witness.try_joined().unwrap();
        assert!(witness.matches_joined(&joined) && joined.registration_released());
        joined.result().unwrap();
        assert!(report.exited_tasks.contains(&joined.id()) && report.failed_tasks.is_empty());
        assert!(grant.native_closed());
        assert_eq!(
            slot.bound_original_budgets()
                .unwrap()
                .carrier
                .snapshot()
                .charged,
            0
        );
        let weak = Arc::downgrade(&child);
        drop(host);
        drop(slot);
        drop(state);
        drop(child);
        drop(grant);
        drop(report);
        assert!(
            weak.upgrade().is_some(),
            "canonical receiver keeps exact child, not only its text"
        );
        drop(canonical);
        assert!(
            weak.upgrade().is_none(),
            "pure source/receiver has no Scope/controller cycle"
        );
    })
    .await
    .unwrap();
}

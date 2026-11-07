//! Actual Scope reception, two real OS sources and independent native/ACK gates.
use super::*;
use std::{
    future::{poll_fn, Future},
    sync::OnceLock,
    task::Poll,
};
use zero_platform_tokio::contract::{
    component::ResourceAccess,
    lifecycle::{ResourceFailurePhase, TaskClass},
    scope::{TaskJoinWitness, TaskRegistrar, TaskScope},
    CleanupFailureReceipt,
};
#[derive(Default)]
struct State {
    resources: OnceLock<Arc<dyn ResourceAccess>>,
    tasks: OnceLock<TaskRegistrar>,
    witness: StdMutex<Option<TaskJoinWitness>>,
    finish: StdMutex<Option<tokio::sync::oneshot::Sender<()>>>,
    entered: Arc<tokio::sync::Notify>,
    grant: Arc<StdMutex<Option<JournalGrant>>>,
}
struct ReceiveFactory {
    state: Arc<State>,
    path: PathBuf,
}
struct ReceiveProtocol {
    state: Arc<State>,
    inner: Box<dyn zero_platform_tokio::contract::lifecycle::DynResourceLifecycle>,
}
impl ComponentFactory for ReceiveFactory {
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
        assert!(self.state.resources.set(context.resources.clone()).is_ok());
        assert!(self.state.tasks.set(context.tasks.clone()).is_ok());
        let built = Factory(self.path.clone(), self.state.grant.clone()).build(options, context)?;
        Ok(ComponentInstance {
            roles: built.roles,
            lifecycle: erase_lifecycle(ReceiveProtocol {
                state: self.state.clone(),
                inner: built.lifecycle,
            }),
        })
    }
}
impl ResourceLifecycle for ReceiveProtocol {
    async fn start(&mut self, end: Instant) -> Result<(), BuildError> {
        self.inner.start(end).await?;
        let (finish, finished) = tokio::sync::oneshot::channel();
        *self.state.finish.lock().unwrap() = Some(finish);
        let entered = self.state.entered.clone();
        let (_, witness) = self
            .state
            .tasks
            .get()
            .unwrap()
            .spawn_tracked(TaskClass::Critical, async move {
                entered.notify_one();
                finished.await.unwrap();
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
        self.inner.shutdown(end).await
    }
}
fn actual_failure(
    path: &std::path::Path,
    reason: Reason,
) -> (Arc<ChannelError>, CleanupFailureReceipt) {
    let actual = Arc::new(std::fs::File::open(path).unwrap_err());
    let receipt = CleanupFailureReceipt::new();
    assert!(receipt.bind_payload(actual.clone()));
    (
        Arc::new(
            ChannelError::new(Stage::Close, reason, Impact::Component)
                .with_source(actual)
                .with_cleanup_failure_receipt(receipt.clone()),
        ),
        receipt,
    )
}
#[tokio::test]
async fn actual_scope_protocol_receiver_preserves_two_os_sources_through_held_ack_and_producer_disposal(
) {
    use crate::component::catalog::supervision::testing::TestContext;
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        let directory = tempfile::tempdir().unwrap();
        let state = Arc::new(State::default());
        let slot = ComponentSlot::from_recipe(
            ComponentId(795),
            ResourceGeneration(2),
            ComponentRecipe::from_validated(
                Arc::new(ReceiveFactory {
                    path: directory.path().join("journal"),
                    state: state.clone(),
                }),
                Arc::new(()),
            ),
            Default::default(),
            "actual-protocol-receiver",
        );
        let host = TestContext::new([&slot]);
        let end = Instant::now() + std::time::Duration::from_secs(3);
        host.drive(slot.start(end)).await.unwrap();
        let resources = state.resources.get().unwrap().clone();
        let tasks = state.tasks.get().unwrap().clone();
        let witness = state.witness.lock().unwrap().as_ref().unwrap().clone();
        let grant = state.grant.lock().unwrap().as_ref().unwrap().clone();
        grant
            .replace(1, |writer| std::io::Write::write_all(writer, b"x"))
            .unwrap();
        let (first, first_receipt) =
            actual_failure(&directory.path().join("first-missing"), Reason::Io);
        let (second, second_receipt) =
            actual_failure(&directory.path().join("second-missing"), Reason::Timeout);
        let mut foreign = TaskScope::suspended_hosted(tokio::runtime::Handle::current(), None);
        assert!(resources
            .receive_protocol_failure(&foreign.registrar().scope_identity(), &first)
            .is_err());
        assert!(!first_receipt.is_acknowledged());
        assert!(slot.protocol_failures.get().is_none());
        let denied = crate::component::instance::resources::NoResources;
        assert!(denied
            .receive_protocol_failure(&tasks.scope_identity(), &first)
            .is_err());
        assert!(!first_receipt.is_acknowledged());
        let canonical = resources
            .receive_protocol_failure(&tasks.scope_identity(), &first)
            .unwrap();
        assert!(first_receipt.is_acknowledged());
        assert!(
            !second_receipt.is_acknowledged(),
            "old acceptance is not late-source acceptance"
        );
        let same = resources
            .receive_protocol_failure(&tasks.scope_identity(), &second)
            .unwrap();
        assert!(Arc::ptr_eq(&same, &canonical));
        drop(same);
        assert!(second_receipt.is_acknowledged());
        assert_eq!(canonical.instance, slot.instance_id());
        assert_eq!(
            canonical.controller,
            ResourceFailurePhase::ProtocolLifecycle
        );
        assert_eq!(slot.protocol_failures.source_count(), 2);
        let live = slot.live.get().unwrap().as_ref().unwrap();
        assert!(Arc::ptr_eq(
            &slot.protocol_failures,
            &live.resources.protocol_failure_receiver()
        ));
        assert!(Arc::ptr_eq(
            &slot.protocol_failures,
            &live.owner.lock().await.protocol_failures
        ));
        assert!(Arc::ptr_eq(
            &slot.observe_resource_fault().unwrap(),
            &canonical
        ));
        assert!(!slot.can_reuse() && !slot.has_started());
        assert!(
            !grant.native_closed() && witness.try_joined().is_none(),
            "local source receipt grants no physical closure"
        );
        state
            .finish
            .lock()
            .unwrap()
            .take()
            .unwrap()
            .send(())
            .unwrap();
        // Consume the actual exit but withhold its unique Host ACK. Callback
        // deferral is deliberately not used as a substitute for receipt custody.
        let receipt = poll_fn(|cx| live.tasks.lock().scope.poll_exit(cx))
            .await
            .unwrap();
        let joined = receipt.joined().clone();
        assert!(witness.matches_joined(&joined));
        let mut closing = Box::pin(host.drive(slot.shutdown(end)));
        poll_fn(|cx| {
            assert!(closing.as_mut().poll(cx).is_pending());
            if grant.native_closed() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        assert!(!joined.registration_released());
        assert_eq!(
            slot.bound_original_budgets()
                .unwrap()
                .carrier
                .snapshot()
                .charged,
            1
        );
        {
            let mut actual = live.tasks.lock();
            // Same persistent Host report owns the real ID before ACK.
            actual.report.exited_tasks.push(receipt.id());
            actual.scope.acknowledge(&receipt).unwrap();
        }
        drop(receipt);
        live.tasks.changed.notify_waiters();
        let report = closing.await.unwrap();
        assert!(report.is_complete(), "actual cleanup: {report:?}");
        assert!(joined.registration_released());
        joined.result().unwrap();
        // Closing/drained registration never rejects a late diagnostic from
        // this still-retained original Host/Scope receiver.
        let late = resources
            .receive_protocol_failure(&tasks.scope_identity(), &second)
            .unwrap();
        assert!(Arc::ptr_eq(&late, &canonical));
        assert_eq!(slot.protocol_failures.source_count(), 2);
        drop(late);
        assert!(grant.native_closed());
        assert_eq!(
            slot.bound_original_budgets()
                .unwrap()
                .carrier
                .snapshot()
                .charged,
            0
        );
        assert_eq!(report.resource_failures.len(), 1);
        assert!(Arc::ptr_eq(&report.resource_failures[0], &canonical));
        assert!(report.failed_tasks.is_empty() && report.exited_tasks.contains(&joined.id()));
        foreign.begin_shutdown();
        assert!(foreign.next_receipt().await.is_none());
        let weak = [Arc::downgrade(&first), Arc::downgrade(&second)];
        drop(host);
        drop(slot);
        drop(state);
        drop(resources);
        drop(tasks);
        drop(grant);
        drop(first);
        drop(second);
        drop(report);
        assert!(weak.iter().all(|source| source.upgrade().is_some()));
        drop(canonical);
        assert!(
            weak.iter().all(|source| source.upgrade().is_none()),
            "same pure receiver has no IO/Scope cycle"
        );
    })
    .await
    .unwrap();
}

//! Original protocol errors retain both independent real OS causes across retries.
use super::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    OnceLock,
};
use zero_platform_tokio::contract::{
    lifecycle::TaskClass,
    scope::{TaskJoinWitness, TaskRegistrar},
    CleanupFailureReceipt,
};
#[derive(Default)]
struct State {
    calls: AtomicUsize,
    errors: [OnceLock<Arc<ChannelError>>; 2],
    receipts: [CleanupFailureReceipt; 2],
    entered: Arc<tokio::sync::Notify>,
    witness: StdMutex<Option<TaskJoinWitness>>,
    grant: Arc<StdMutex<Option<JournalGrant>>>,
}
struct ProtocolFactory {
    path: PathBuf,
    state: Arc<State>,
}
struct Protocol {
    inner: Box<dyn zero_platform_tokio::contract::lifecycle::DynResourceLifecycle>,
    tasks: TaskRegistrar,
    state: Arc<State>,
    path: PathBuf,
}
impl ComponentFactory for ProtocolFactory {
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
        let tasks = context.tasks.clone();
        let original =
            Factory(self.path.clone(), self.state.grant.clone()).build(options, context)?;
        Ok(ComponentInstance {
            roles: original.roles,
            lifecycle: erase_lifecycle(Protocol {
                inner: original.lifecycle,
                tasks,
                state: self.state.clone(),
                path: self.path.clone(),
            }),
        })
    }
}
impl ResourceLifecycle for Protocol {
    async fn start(&mut self, end: Instant) -> Result<(), BuildError> {
        self.inner.start(end).await?;
        let entered = self.state.entered.clone();
        let stop = self.tasks.cancellation();
        let (_, witness) = self
            .tasks
            .spawn_tracked(TaskClass::Critical, async move {
                entered.notify_one();
                stop.cancelled().await;
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
        let index = self.state.calls.fetch_add(1, Ordering::AcqRel);
        if index < 64 {
            let position = index / 32;
            let source = self.state.errors[position]
                .get_or_init(|| {
                    let error = std::fs::File::open(
                        self.path.with_extension(format!("missing-{position}")),
                    )
                    .expect_err("real missing-file OS call must fail inside this actual lifecycle");
                    let payload = Arc::new(error);
                    let receipt = self.state.receipts[position].clone();
                    assert!(receipt.bind_payload(payload.clone()));
                    Arc::new(
                        ChannelError::new(
                            Stage::Close,
                            if position == 0 {
                                Reason::Io
                            } else {
                                Reason::Timeout
                            },
                            Impact::Component,
                        )
                        .with_source(payload)
                        .with_cleanup_failure_receipt(receipt),
                    )
                })
                .clone();
            return Err(ChannelError::retained(source));
        }
        self.inner.shutdown(end).await
    }
}
#[tokio::test]
async fn two_actual_lifecycle_causes_share_canonical_report_and_repeated_wrappers_do_not_grow() {
    use crate::component::catalog::supervision::testing::TestContext;
    let directory = tempfile::tempdir().unwrap();
    let state = Arc::new(State::default());
    let slot = ComponentSlot::from_recipe(
        ComponentId(793),
        ResourceGeneration(2),
        ComponentRecipe::from_validated(
            Arc::new(ProtocolFactory {
                path: directory.path().join("state"),
                state: state.clone(),
            }),
            Arc::new(()),
        ),
        Default::default(),
        "original-protocol",
    );
    let host = TestContext::new([&slot]);
    let end = Instant::now() + std::time::Duration::from_secs(3);
    host.drive(slot.start(end)).await.unwrap();
    let grant = state.grant.lock().unwrap().as_ref().unwrap().clone();
    grant
        .replace(1, |writer| std::io::Write::write_all(writer, b"x"))
        .unwrap();
    let mut receiver = None;
    let mut received = CloseReport::default();
    for index in 0..64 {
        let report = host.drive(slot.shutdown(end)).await.unwrap();
        assert!(
            !report.is_complete(),
            "an error never proves this protocol controller closed"
        );
        assert!(
            grant.native_closed(),
            "actual declaration close is independent from the protocol error"
        );
        assert!(
            slot.task_fault().is_none(),
            "resource IO cannot invent a task failure"
        );
        let fault = report
            .resource_failures
            .iter()
            .find(|fault| {
                fault.controller ==
            zero_platform_tokio::contract::lifecycle::ResourceFailurePhase::ProtocolLifecycle
            })
            .unwrap();
        if let Some(original) = &receiver {
            assert!(Arc::ptr_eq(original, fault));
        } else {
            receiver = Some(fault.clone());
        }
        assert!(fault.acknowledge_retained(receiver.as_ref().unwrap()));
        assert!(
            state.receipts[index / 32].is_acknowledged(),
            "the exact strongly retained original source is locally accepted"
        );
        if index < 32 {
            assert!(
                !state.receipts[1].is_acknowledged(),
                "the old canonical acceptance cannot acknowledge a later original receipt"
            );
        }
        received.append(report);
    }
    let report = host.drive(slot.shutdown(end)).await.unwrap();
    assert!(report.is_complete());
    let fault = receiver.unwrap();
    assert!(report
        .resource_failures
        .iter()
        .any(|same| Arc::ptr_eq(same, &fault)));
    let joined = state
        .witness
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .try_joined()
        .unwrap();
    assert!(joined.registration_released());
    joined.result().unwrap();
    received.append(report);
    assert!(received.exited_tasks.contains(&joined.id()));
    assert_eq!(
        slot.bound_original_budgets()
            .unwrap()
            .carrier
            .snapshot()
            .charged,
        0
    );
    // Receiver has exactly the two actual causes, not 64 observations/wrappers.
    assert_eq!(
        slot.live
            .get()
            .unwrap()
            .as_ref()
            .unwrap()
            .owner
            .lock()
            .await
            .protocol_failures
            .source_count(),
        2
    );
    let weak: Vec<_> = state
        .errors
        .iter()
        .map(|source| Arc::downgrade(source.get().unwrap()))
        .collect();
    drop(host);
    drop(slot);
    drop(state);
    drop(grant);
    drop(received);
    assert!(
        weak.iter().all(|source| source.upgrade().is_some()),
        "same actual receiver retains both OS sources after producer disposal"
    );
    drop(fault);
    assert!(
        weak.iter().all(|source| source.upgrade().is_none()),
        "no canonical fault/source cycle"
    );
}

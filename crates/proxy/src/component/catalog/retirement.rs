//! Declared completion and sticky task faults are different retained facts.
//! Native declarations remain unconfirmed without provider-level physical proof.
use super::*;
use std::collections::BTreeSet;
use zero_core::component::ComponentInstanceId;

impl ComponentPlan {
    /// Confirms the actual task/lifecycle subset without native declarations.
    /// Native declarations keep their real partial reports but require a future
    /// provider proof. Transferred users require separate owners in all cases.
    pub(super) async fn confirm_instances(
        &self,
        instances: &BTreeSet<ComponentInstanceId>,
        deadline: tokio::time::Instant,
    ) -> Result<(), ChannelError> {
        let _closing = tokio::time::timeout_at(deadline, self.closing.lock())
            .await
            .map_err(|_| invalid(Reason::Timeout, "component close collection remains owned"))?;
        let retiring: Vec<_> = self
            .order
            .iter()
            .rev()
            .filter(|slot| instances.contains(&slot.instance_id()))
            .collect();
        if retiring.len() != instances.len() {
            return Err(invalid(
                Reason::InvalidConfig,
                "retirement contains an unknown owner",
            ));
        }
        // Revoke the whole exact subset before its first cleanup suspension.
        for slot in &retiring {
            slot.revoke();
        }
        let mut pending = None;
        for slot in retiring {
            let identity = slot.instance_id();
            if self
                .confirmed_declared_close
                .lock()
                .expect("component close proof poisoned")
                .contains(&identity)
            {
                continue;
            }
            match slot.shutdown(deadline).await {
                Ok(mut report) => {
                    // Persist the actual failure first, then declared completion,
                    // without an await or a caller-owned temporary in between.
                    {
                        let mut failures =
                            self.failures.lock().expect("component failures poisoned");
                        for failure in report.failed_tasks.drain(..) {
                            failures.entry(failure.id).or_insert_with(|| {
                                Arc::new(plan::PlanTaskFailure::new(failure, identity))
                            });
                        }
                    }
                    let complete = report.is_complete();
                    if complete && slot.recipe.spec().resources.is_empty() {
                        self.confirmed_declared_close
                            .lock()
                            .expect("component close proof poisoned")
                            .insert(identity);
                    } else if complete {
                        // Complete currently includes file-job joins without an
                        // explicit native File close ledger. No resourceful
                        // production is enabled; retain every actual receipt,
                        // but never turn this into a full native close proof.
                        report.unfinished.push(
                            zero_platform_tokio::contract::lifecycle::Unfinished::Io(
                                "native declaration lacks provider physical closure proof".into(),
                            ),
                        );
                        pending.get_or_insert_with(|| {
                            let mut error = invalid(
                                Reason::Unsupported,
                                "native declaration lacks provider physical closure proof",
                            );
                            error.stage = zero_platform_tokio::contract::Stage::Close;
                            error.delivery = zero_platform_tokio::contract::DeliveryState::Unknown;
                            error
                        });
                    } else {
                        pending.get_or_insert_with(|| {
                            invalid(Reason::Timeout, "component cleanup remains unfinished")
                        });
                    }
                    let mut evidence = self
                        .close_evidence
                        .lock()
                        .expect("component close evidence poisoned");
                    let retained = evidence.entry(identity).or_default();
                    retained.unfinished.clear();
                    retained.append(report);
                }
                Err(error) => {
                    pending.get_or_insert(error);
                }
            }
        }
        pending.map_or(Ok(()), Err)
    }

    pub(super) fn confirmed(&self, instances: &BTreeSet<ComponentInstanceId>) -> bool {
        let closed = self
            .confirmed_declared_close
            .lock()
            .expect("component close proof poisoned");
        instances.is_subset(&closed)
    }

    pub(super) fn task_faults(
        &self,
        instances: &BTreeSet<ComponentInstanceId>,
    ) -> Vec<Arc<plan::PlanTaskFailure>> {
        self.failures
            .lock()
            .expect("component failures poisoned")
            .values()
            .filter(|failure| instances.contains(&failure.1))
            .cloned()
            .collect()
    }
}

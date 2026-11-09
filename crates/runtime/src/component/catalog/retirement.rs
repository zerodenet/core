//! Declared completion and sticky task faults are different retained facts.
//! Native declarations remain unconfirmed without provider-level physical proof.
use super::*;
use std::collections::BTreeSet;
use zero_core::component::ComponentInstanceId;

impl ComponentPlan {
    /// The slot combines actual protocol/task receipts with declaration-wide
    /// provider evidence before returning a complete report. Unknown native or
    /// transferred users therefore remain charged here.
    pub(super) async fn confirm_instances(
        &self,
        instances: &BTreeSet<ComponentInstanceId>,
        deadline: tokio::time::Instant,
    ) -> Result<(), ChannelError> {
        let _closing = tokio::time::timeout_at(deadline, self.closing.lock())
            .await
            .map_err(|_| invalid(Reason::Timeout, "component close collection remains owned"))?;
        let nodes: BTreeMap<_, _> = self
            .order
            .iter()
            .filter(|slot| instances.contains(&slot.instance_id()))
            .map(|slot| (slot.instance_id(), slot.clone()))
            .collect();
        let ordered =
            super::replacement::order::close_order(&nodes, &self.borrowing.loan_snapshot().1)?;
        let retiring: Vec<_> = ordered.iter().map(|id| &nodes[id]).collect();
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
        let mut blocked: BTreeSet<ComponentInstanceId> = BTreeSet::new();
        for slot in retiring {
            let identity = slot.instance_id();
            if blocked.contains(&identity) {
                blocked.extend(self.retirement_dependencies(slot));
                continue;
            }
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
                            failures
                                .entry(plan::FailureKey::Task(failure.id))
                                .or_insert_with(|| {
                                    Arc::new(plan::PlanFailure::new(failure, identity))
                                });
                        }
                    }
                    let mut actual_resource_fault = None;
                    for failure in &report.resource_failures {
                        if failure.instance != identity {
                            return Err(invalid(
                                Reason::StalePlan,
                                "resource failure belongs to another actual slot",
                            ));
                        }
                        let actual = {
                            let mut failures =
                                self.failures.lock().expect("component failures poisoned");
                            failures
                                .entry(plan::FailureKey::Resource(identity, failure.controller))
                                .or_insert_with(|| {
                                    Arc::new(plan::PlanFailure::resource(failure.clone()))
                                })
                                .clone()
                        };
                        actual_resource_fault.get_or_insert_with(|| actual.clone());
                        // Persist the exact source in the existing canonical archive
                        // before acknowledging the original provider receipt.
                        // Publication uses the same row after full real close.
                        if !actual.receive_resource_ownership(failure) {
                            return Err(invalid(
                                Reason::StalePlan,
                                "resource failure differs from its actual retained archive row",
                            )
                            .with_source(Arc::new(plan::PlanFailure::resource(failure.clone()))));
                        }
                    }
                    let complete = report.is_complete();
                    if complete {
                        self.confirmed_declared_close
                            .lock()
                            .expect("component close proof poisoned")
                            .insert(identity);
                    } else {
                        blocked.extend(self.retirement_dependencies(slot));
                        pending.get_or_insert_with(|| {
                            if let Some(actual) = actual_resource_fault {
                                return actual.retained();
                            }
                            let mut error =
                                invalid(Reason::Timeout, "component cleanup remains unfinished");
                            error.stage = zero_platform_tokio::contract::Stage::Close;
                            error.component = Some(slot.id);
                            error
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
                    blocked.extend(self.retirement_dependencies(slot));
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

    pub(super) fn retirement_faults(
        &self,
        instances: &BTreeSet<ComponentInstanceId>,
    ) -> Vec<Arc<plan::PlanFailure>> {
        self.failures
            .lock()
            .expect("component failures poisoned")
            .values()
            .filter(|failure| instances.contains(&failure.instance()))
            .cloned()
            .collect()
    }
}

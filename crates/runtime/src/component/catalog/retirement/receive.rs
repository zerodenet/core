//! Persist original reports/faults before another group member can suspend.
use super::*;
use zero_platform_tokio::contract::lifecycle::CloseReport;
impl ComponentPlan {
    pub(super) fn receive_instance_close(
        &self,
        slot: &ComponentSlot,
        result: Result<CloseReport, ChannelError>,
    ) -> Result<(), ChannelError> {
        let mut report = result?;
        let identity = slot.instance_id();
        // Preserve even a malformed report verbatim; it cannot qualify closure
        // or acknowledge an original belonging to another instance/controller.
        if report
            .resource_failures
            .iter()
            .any(|failure| failure.instance != identity)
        {
            self.close_evidence
                .lock()
                .expect("component close evidence poisoned")
                .entry(identity)
                .or_default()
                .append(report);
            return Err(invalid(
                Reason::StalePlan,
                "resource failure belongs to another actual slot",
            ));
        }
        {
            let mut failures = self.failures.lock().expect("component failures poisoned");
            for failure in report.failed_tasks.drain(..) {
                failures
                    .entry(plan::FailureKey::Task(failure.id))
                    .or_insert_with(|| Arc::new(plan::PlanFailure::new(failure, identity)));
            }
        }
        let mut actual_fault = None;
        let mut mismatched_source = None;
        for failure in &report.resource_failures {
            let actual = {
                let mut failures = self.failures.lock().expect("component failures poisoned");
                failures
                    .entry(plan::FailureKey::Resource(identity, failure.controller))
                    .or_insert_with(|| Arc::new(plan::PlanFailure::resource(failure.clone())))
                    .clone()
            };
            actual_fault.get_or_insert_with(|| actual.clone());
            if !actual.receive_resource_ownership(failure) {
                mismatched_source = Some(Arc::new(plan::PlanFailure::resource(failure.clone())));
                break;
            }
        }
        if let Some(source) = mismatched_source {
            self.close_evidence
                .lock()
                .expect("component close evidence poisoned")
                .entry(identity)
                .or_default()
                .append(report);
            return Err(invalid(
                Reason::StalePlan,
                "resource failure differs from its actual retained archive row",
            )
            .with_source(source));
        }
        let complete = report.is_complete();
        if complete {
            self.confirmed_declared_close
                .lock()
                .expect("component close proof poisoned")
                .insert(identity);
        }
        {
            let mut evidence = self
                .close_evidence
                .lock()
                .expect("component close evidence poisoned");
            let retained = evidence.entry(identity).or_default();
            retained.unfinished.clear();
            retained.append(report);
        }
        if complete {
            Ok(())
        } else {
            Err(actual_fault.map_or_else(
                || {
                    let mut error =
                        invalid(Reason::Timeout, "component cleanup remains unfinished");
                    error.stage = zero_platform_tokio::contract::Stage::Close;
                    error.component = Some(slot.id);
                    error
                },
                |actual| actual.retained(),
            ))
        }
    }
}

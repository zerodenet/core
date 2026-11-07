//! Actual close-report rows share the original controller's pure receiver.
use super::*;
use zero_platform_tokio::contract::lifecycle::TaskFailure;
impl DeclaredReceiver {
    pub(in crate::component) fn close_report(
        &self,
        instance: zero_core::component::ComponentInstanceId,
        report: &mut CloseReport,
    ) -> Result<Option<Arc<ResourceFailure>>, ChannelError> {
        // A real task must carry its raw original source. Retaining this
        // receiver's own source through a task row would create a source cycle.
        // Reject before touching any caller-owned row; it remains recoverable.
        if report
            .failed_tasks
            .iter()
            .any(|row| self.owns_source(&row.error))
        {
            return Err(super::super::error(
                instance.component,
                Reason::InvalidConfig,
                "protocol task report refers to its own canonical receiver",
            ));
        }
        for row in &mut report.failed_tasks {
            // The report keeps the same decision fields and exact retained
            // input Arc. Distinct late sources with one TaskId are not erased.
            let placeholder = ChannelError::new(
                zero_platform_tokio::contract::Stage::Close,
                Reason::Internal,
                zero_platform_tokio::contract::Impact::Component,
            );
            let source = match row
                .error
                .retained_source()
                .filter(|source| same_decisions(&row.error, source))
            {
                Some(source) => source.clone(),
                None => {
                    let source = Arc::new(std::mem::replace(&mut row.error, placeholder));
                    row.error = ChannelError::retained(source.clone());
                    source
                }
            };
            let mut causes = self
                .causes
                .sources
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !causes.iter().any(|cause| {
                matches!(cause,
                Cause::ReportTask { id, class, source: same }
                if *id == row.id && *class == row.class && same_report_source(same, &source))
            }) {
                causes.push(Cause::ReportTask {
                    id: row.id,
                    class: row.class,
                    source: source.clone(),
                });
            }
            drop(causes);
            self.causes.first.get_or_init(|| source.clone());
            self.canonical(instance);
            if let Some(receipt) = source.cleanup_failure_receipt() {
                let _ = receipt.acknowledge_retained(&source);
            }
        }
        for child in &report.resource_failures {
            // Returning the receiver's own RF never inserts it back into Causes.
            if self.owns_source(child.source.as_ref()) {
                let _ = child.acknowledge_retained(child);
                continue;
            }
            let mut causes = self
                .causes
                .sources
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !causes.iter().any(|cause| {
                matches!(cause,
                Cause::ReportResource(same) if Arc::ptr_eq(same, child))
            }) {
                causes.push(Cause::ReportResource(child.clone()));
            }
            drop(causes);
            self.causes.first.get_or_init(|| child.source.clone());
            self.canonical(instance);
            // All actual RF metadata, including its original-source ledger,
            // is strong before accepting that exact child. No Scope ACK here.
            let _ = child.acknowledge_retained(child);
        }
        Ok(self.get())
    }
    fn owns_source(&self, source: &ChannelError) -> bool {
        let Some(canonical) = self.canonical.get() else {
            return false;
        };
        let mut node = source;
        loop {
            if std::ptr::eq(node, canonical.source.as_ref())
                || node.cleanup_failure_receipt().is_some_and(|receipt| {
                    canonical
                        .source
                        .cleanup_failure_receipt()
                        .is_some_and(|same| same.same_receipt(receipt))
                })
            {
                return true;
            }
            if let Some(retained) = node.retained_source() {
                node = retained.as_ref();
                continue;
            }
            // Inspect only concrete ChannelError forwarding forms, never call
            // arbitrary producer source() under a receiver lock or walk an
            // opaque payload. Those payloads still must obey the no-cycle contract.
            let Some(source) = std::error::Error::source(node) else {
                return false;
            };
            if let Some(retained) = source.downcast_ref::<Arc<ChannelError>>() {
                node = retained.as_ref();
            } else if let Some(retained) = source.downcast_ref::<ChannelError>() {
                node = retained;
            } else {
                return false;
            }
        }
    }
    pub(in crate::component) fn append_task_rows(&self, report: &mut CloseReport) {
        let causes = self
            .causes
            .sources
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for cause in causes.iter() {
            match cause {
                Cause::ReportTask { id, class, source } => {
                    if !report.failed_tasks.iter().any(|same| {
                        same.id == *id
                            && same.class == *class
                            && same
                                .error
                                .retained_source()
                                .is_some_and(|same| same_report_source(same, source))
                    }) {
                        report.failed_tasks.push(TaskFailure {
                            id: *id,
                            class: *class,
                            error: ChannelError::retained(source.clone()),
                        });
                    }
                }
                _ => {}
            }
        }
    }
}

fn same_decisions(first: &ChannelError, second: &ChannelError) -> bool {
    first.stage == second.stage
        && first.reason == second.reason
        && first.impact == second.impact
        && first.component == second.component
        && first.session == second.session
        && first.delivery == second.delivery
        && first.retry == second.retry
        && match (
            first.cleanup_failure_receipt(),
            second.cleanup_failure_receipt(),
        ) {
            (Some(first), Some(second)) => first.same_receipt(second),
            (None, None) => true,
            _ => false,
        }
}

fn same_report_source(first: &Arc<ChannelError>, second: &Arc<ChannelError>) -> bool {
    match (
        first.cleanup_failure_receipt(),
        second.cleanup_failure_receipt(),
    ) {
        (Some(first), Some(second)) => first.same_receipt(second),
        (None, None) => same_source(first, second),
        _ => false,
    }
}

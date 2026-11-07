//! Same actual original's report custody across cancelled close awaits.
use super::*;
use std::ops::{Deref, DerefMut};
pub(super) struct PendingReport<'a> {
    storage: &'a Mutex<CloseReport>,
    report: CloseReport,
}
impl<'a> PendingReport<'a> {
    pub(super) fn take(storage: &'a Mutex<CloseReport>) -> Self {
        let report = std::mem::take(&mut *storage.lock().expect("DNS original report poisoned"));
        Self { storage, report }
    }
    pub(super) fn append(&mut self, mut next: CloseReport) {
        for row in &mut next.failed_tasks {
            if row.error.retained_source().is_none() {
                let actual = Arc::new(std::mem::replace(
                    &mut row.error,
                    crate::component::instance::invalid(
                        zero_platform_tokio::contract::Reason::Internal,
                        "unexposed task row move",
                    ),
                ));
                row.error = BuildError::retained(actual);
            }
        }
        for row in next.failed_tasks {
            if !self.report.failed_tasks.iter().any(|old| {
                old.id == row.id && old.class == row.class && same_source(&old.error, &row.error)
            }) {
                self.report.failed_tasks.push(row);
            }
        }
        for child in next.resource_failures {
            if !self
                .report
                .resource_failures
                .iter()
                .any(|old| Arc::ptr_eq(old, &child))
            {
                self.report.resource_failures.push(child);
            }
        }
        append_unique(&mut self.report.exited_tasks, next.exited_tasks);
        append_unique(&mut self.report.closed_io, next.closed_io);
        append_unique(&mut self.report.released_resources, next.released_resources);
        append_unique(
            &mut self.report.recovered_resources,
            next.recovered_resources,
        );
        append_unique(&mut self.report.unfinished, next.unfinished);
    }
    pub(super) fn finish(&mut self) -> CloseReport {
        std::mem::take(&mut self.report)
    }
}
fn append_unique<T: PartialEq>(current: &mut Vec<T>, next: Vec<T>) {
    for row in next {
        if !current.contains(&row) {
            current.push(row);
        }
    }
}
fn same_source(first: &BuildError, second: &BuildError) -> bool {
    match (
        first.cleanup_failure_receipt(),
        second.cleanup_failure_receipt(),
    ) {
        (Some(first), Some(second)) => return first.same_receipt(second),
        (None, None) => {}
        _ => return false,
    }
    let (Some(mut first), Some(mut second)) = (first.retained_source(), second.retained_source())
    else {
        return std::ptr::eq(first, second);
    };
    while let Some(original) = first.retained_source() {
        first = original;
    }
    while let Some(original) = second.retained_source() {
        second = original;
    }
    Arc::ptr_eq(first, second)
}
impl Deref for PendingReport<'_> {
    type Target = CloseReport;
    fn deref(&self) -> &CloseReport {
        &self.report
    }
}
impl DerefMut for PendingReport<'_> {
    fn deref_mut(&mut self) -> &mut CloseReport {
        &mut self.report
    }
}
impl Drop for PendingReport<'_> {
    fn drop(&mut self) {
        // NativeUse.logical serializes the same controller through this guard's
        // lifetime. No provider callback/error destructor runs under storage lock.
        let previous = {
            let mut stored = self.storage.lock().expect("DNS original report poisoned");
            std::mem::replace(&mut *stored, std::mem::take(&mut self.report))
        };
        // No other close can publish until logical is unlocked after this guard.
        debug_assert!(
            previous.exited_tasks.is_empty()
                && previous.failed_tasks.is_empty()
                && previous.resource_failures.is_empty()
                && previous.closed_io.is_empty()
                && previous.released_resources.is_empty()
                && previous.recovered_resources.is_empty()
                && previous.unfinished.is_empty()
        );
        drop(previous);
    }
}

#[cfg(test)]
mod tests;

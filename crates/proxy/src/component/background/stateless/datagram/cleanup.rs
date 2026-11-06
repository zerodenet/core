//! Persist each original branch before returning to its sibling's join future.
use super::*;
#[derive(Default)]
struct Segment {
    report: CloseReport,
    complete: bool,
}
#[derive(Default)]
pub(super) struct Cleanup {
    parts: [Segment; 2],
    faults: Option<Arc<FaultSources>>,
    receipt: Option<zero_platform_tokio::contract::CleanupFailureReceipt>,
}
impl Cleanup {
    pub(super) fn completed(&self, index: usize) -> bool {
        self.parts[index].complete
    }
    /// Result ownership enters the original controller before the branch is
    /// Ready. Cancellation of a sibling cannot discard reports or error Arcs.
    pub(super) fn accept(
        &mut self,
        index: usize,
        outcome: Result<CloseReport, ChannelError>,
    ) -> Option<ChannelError> {
        let part = &mut self.parts[index];
        match outcome {
            Ok(mut report) => {
                part.complete = report.is_complete();
                // Only this segment's new positive observation supersedes its
                // own previous unfinished state. All accepted receipts remain.
                part.report.unfinished = std::mem::take(&mut report.unfinished);
                part.report.append(report);
            }
            Err(error) => {
                let faults = self
                    .faults
                    .get_or_insert_with(|| Arc::new(FaultSources::default()));
                if faults.sources[index].get().is_some() {
                    return Some(error);
                }
                let source = Arc::new(error);
                faults.sources[index]
                    .set(source.clone())
                    .expect("same controller cleanup lock serializes first failure");
                faults.first.get_or_init(|| index);
                self.receipt.get_or_insert_with(|| {
                    let receipt = zero_platform_tokio::contract::CleanupFailureReceipt::default();
                    assert!(
                        receipt.bind_payload(faults.clone()),
                        "one stable original fault record"
                    );
                    receipt
                });
                // This exact original error Arc is now durably installed in
                // the controller's own finite row before acknowledging receipt.
                if let Some(receipt) = source.cleanup_failure_receipt() {
                    receipt.acknowledge_retained(&source);
                }
            }
        }
        None
    }
    pub(super) fn snapshot(
        &mut self,
        original_complete: bool,
    ) -> Result<CloseReport, ChannelError> {
        // The receiving Entry/LoanOwner must first strongly hold this exact
        // structured record. A later close invocation is never an ACK.
        if let Some(faults) = &self.faults {
            let receipt = self
                .receipt
                .as_ref()
                .expect("actual fault owns one receipt");
            if !receipt.is_acknowledged() {
                return Err(
                    crate::component::instance::retained(faults.primary().clone())
                        .with_source(faults.clone())
                        .with_cleanup_failure_receipt(receipt.clone()),
                );
            }
        }
        if original_complete && self.parts.iter().all(|part| part.complete) {
            let mut report = std::mem::take(&mut self.parts[0].report);
            report.append(std::mem::take(&mut self.parts[1].report));
            return Ok(report);
        }
        // Completed receipts stay strongly retained until a complete handoff;
        // an incomplete snapshot only publishes actual current debt, not them.
        let mut report = CloseReport::default();
        for (index, part) in self.parts.iter().enumerate() {
            report
                .unfinished
                .extend(part.report.unfinished.iter().cloned());
            if !part.complete && part.report.unfinished.is_empty() {
                report.unfinished.push(Unfinished::Resource(format!(
                    "Datagram cleanup segment {index} has no positive completion"
                )));
            }
        }
        if !original_complete {
            report.unfinished.push(Unfinished::Resource(
                "Datagram original or actual caller registration ACK remains pending".into(),
            ));
        }
        Ok(report)
    }
}
/// One immutable receiving row can acquire a late second cause, without
/// retaining the Transfer, physical IO or cleanup reports in diagnostic data.
#[derive(Debug, Default)]
struct FaultSources {
    sources: [std::sync::OnceLock<Arc<ChannelError>>; 2],
    first: std::sync::OnceLock<usize>,
}
impl FaultSources {
    fn primary(&self) -> &Arc<ChannelError> {
        self.sources[*self.first.get().expect("actual first fault")]
            .get()
            .expect("actual installed fault")
    }
}
impl std::fmt::Display for FaultSources {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self.primary().as_ref(), f)?;
        let secondary = 1 - *self.first.get().unwrap();
        if let Some(source) = self.sources[secondary].get() {
            write!(f, "; other original cleanup: {source}")?;
        }
        Ok(())
    }
}
impl std::error::Error for FaultSources {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.primary().as_ref())
    }
}

#[cfg(test)]
mod tests;

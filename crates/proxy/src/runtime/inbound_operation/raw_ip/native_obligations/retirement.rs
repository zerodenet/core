//! Actual native/caller census continues while the same Source consumes exits.
use super::*;
use zero_platform_tokio::contract::{
    lifecycle::{CloseReport, ResourceLifecycle, Unfinished},
    CloseReason,
};
#[derive(Debug)]
pub(super) struct SharedSource(pub(super) Source);
impl std::fmt::Display for SharedSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self.0.as_ref(), formatter)
    }
}
impl std::error::Error for SharedSource {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
impl ResourceLifecycle for NativeObligations {
    async fn start(&mut self, _: Instant) -> Result<(), ChannelError> {
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {
        self.stop();
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.stop();
        loop {
            self.settle_joined(deadline).await?;
            let report = self.close_report()?;
            if report.is_complete() || !self.has_covered_gateway() || Instant::now() >= deadline {
                return Ok(report);
            }
            // Original peer/provider receipts are produced by the containing
            // Scope consumer concurrently. Keep this same owner while they join.
            tokio::time::sleep_until(
                deadline.min(Instant::now() + std::time::Duration::from_millis(2)),
            )
            .await;
        }
    }
}
impl NativeObligations {
    fn close_report(&self) -> Result<CloseReport, ChannelError> {
        // Clone only bounded registration identities before querying original
        // carrier/caller receipts; no ledger lock surrounds other owners.
        let registrations: Vec<_> = self
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .records
            .keys()
            .map(|id| NativeRegistration {
                owner: Arc::downgrade(&self.0),
                id: *id,
            })
            .collect();
        let locally_closed_carriers = registrations
            .iter()
            .filter(|registration| {
                registration
                    .carrier_completion()
                    .is_some_and(|receipt| receipt.matches(registration))
            })
            .count();
        let state = self.0.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(source) = &state.first_source {
            return Err(rejected(Reason::Io).with_source(SharedSource(source.clone())));
        }
        let mut report = CloseReport::default();
        if !state.records.is_empty() {
            let entered = state
                .records
                .values()
                .filter(|record| record.entered)
                .count();
            let retiring = state
                .records
                .values()
                .filter(|record| record.retiring)
                .count();
            report.unfinished.push(Unfinished::Io(format!(
                "{} raw-IP child obligations ({entered} unaccounted, {retiring} retiring, {locally_closed_carriers} locally confirmed carriers) lack complete gateway closure proof",
                state.records.len()
            )));
        }
        Ok(report)
    }
}

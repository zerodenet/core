//! Native and foreign branches persist actual reports before the other can wait.
use super::*;
impl Transfer {
    /// A demand without a deadline may drive retirement, but never owns an
    /// unbounded wait. The outer timer wins before inner close timers can turn
    /// an expired grace period into a retained cleanup fault. Segment receipts
    /// stay on this controller even if cancellation drops the joined future.
    pub(super) async fn close_with_grace(
        &self,
        cancel: &CancellationToken,
        end: Instant,
    ) -> Result<CloseReport, ChannelError> {
        let report = self.try_close()?;
        if report.is_complete() || !self.has_incomplete_cleanup_segments() {
            return Ok(report);
        }
        tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(crate::component::instance::invalid(
                Reason::Cancelled,
                "carrier cleanup demand cancelled",
            )),
            _ = tokio::time::sleep_until(end) => self.try_close(),
            waited = self.close(end) => waited,
        }
    }
    pub(super) fn try_close(&self) -> Result<CloseReport, ChannelError> {
        self.stop.cancel();
        self.request_pending_retirement();
        if !self.cleanup.lock().unwrap().completed(0) {
            let result = self.try_close_original();
            let duplicate = { self.cleanup.lock().unwrap().accept(0, result) };
            drop(duplicate);
        }
        if !self.cleanup.lock().unwrap().completed(1) {
            let result = self.try_close_pending();
            let duplicate = { self.cleanup.lock().unwrap().accept(1, result) };
            drop(duplicate);
        }
        let complete = self.finished();
        self.cleanup.lock().unwrap().snapshot(complete)
    }
    pub(super) async fn close(&self, end: Instant) -> Result<CloseReport, ChannelError> {
        self.stop.cancel();
        self.request_pending_retirement();
        let original = async {
            if self.cleanup.lock().unwrap().completed(0) {
                return;
            }
            let result = self.close_original(end).await;
            let duplicate = { self.cleanup.lock().unwrap().accept(0, result) };
            drop(duplicate);
        };
        let foreign = async {
            if self.cleanup.lock().unwrap().completed(1) {
                return;
            }
            let result = self.close_pending(end).await;
            let duplicate = { self.cleanup.lock().unwrap().accept(1, result) };
            drop(duplicate);
        };
        tokio::join!(original, foreign);
        let complete = self.finished();
        self.cleanup.lock().unwrap().snapshot(complete)
    }
}

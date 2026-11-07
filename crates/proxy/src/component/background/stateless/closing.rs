//! Native and foreign branches persist actual reports before the other can wait.
use super::*;
impl Transfer {
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

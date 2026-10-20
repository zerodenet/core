//! The same actual peer is already joined by the containing child registry.
use super::*;
impl GrantedBoundPeerResource {
    pub(super) async fn close_joined(
        &mut self,
        deadline: Instant,
    ) -> Result<CloseReport, ChannelError> {
        self.provider.revoke();
        let mut current_failure = None;
        if !self.protocol_closed {
            match self.inner.shutdown(deadline).await {
                Ok(report) => {
                    self.protocol_closed = report.is_complete();
                    self.protocol_report.unfinished.clear();
                    self.protocol_report.append(report);
                }
                Err(error) => {
                    let source = Arc::new(error);
                    self.remember(source.clone());
                    current_failure = Some(source);
                }
            }
        }
        // A mapping/queue failure must not skip the actual provider's native
        // close attempt. Only it can confirm its original descriptors.
        let native_closed = match self.provider.shutdown_io() {
            Ok(report) => {
                let complete = report.is_complete();
                self.native_report.unfinished.clear();
                self.native_report.append(report);
                complete
            }
            Err(error) => {
                self.native_report.unfinished.clear();
                self.native_report.append(self.provider.take_closed_io());
                let source = Arc::new(error);
                self.remember(source.clone());
                current_failure.get_or_insert(source);
                false
            }
        };
        if let Some(source) = self.provider.fault() {
            self.remember(source);
        }
        if !self.protocol_closed || !native_closed {
            return Err(current_failure.map_or_else(
                || {
                    error(
                        Reason::Timeout,
                        "actual peer mapping/native cleanup remains unconfirmed",
                    )
                },
                retained,
            ));
        }
        // The persistent group retains the original failure separately from
        // these physical confirmations. This permits real closed-owner release
        // while keeping new admission faulted until the host handles the fault.
        let mut report = std::mem::take(&mut self.protocol_report);
        report.append(std::mem::take(&mut self.native_report));
        Ok(report)
    }
}

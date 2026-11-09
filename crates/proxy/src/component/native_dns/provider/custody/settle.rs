use super::*;
impl NativeUse {
    pub(in crate::component::native_dns::provider) async fn settle(
        &self,
        end: tokio::time::Instant,
    ) -> std::io::Result<CloseReport> {
        let mut logical = tokio::time::timeout_at(end, self.logical.lock())
            .await
            .map_err(super::retirement::pending_deadline)?;
        // Declared after the actual controller guard: cancellation returns the
        // partial report to this same original before unlocking that controller.
        let mut report = report::PendingReport::take(&self.pending_report);
        report.unfinished.clear();
        if let Some(owner) = logical.as_mut() {
            let closed = match owner.close(end).await {
                Ok(closed) => closed,
                Err(source) => {
                    return Err(self.receiver.retain_error(std::io::Error::other(source))?)
                }
            };
            let complete =
                closed.is_complete() && owner.closed().is_some_and(|closed| closed.matches(owner));
            report.append(closed);
            // Storage has no locked std Mutex here. Receive before any next
            // await, including on partial output; this acknowledges no driver.
            self.receiver.receive_report(&mut report)?;
            if !complete {
                return Err(retirement::pending_incomplete(
                    "DNS protocol carrier view remains retained",
                ));
            }
        }
        let closed = match self.original.retire(end).await {
            Ok(closed) => closed,
            Err(source) => {
                return Err(self.receiver.retain_error(std::io::Error::other(source))?)
            }
        };
        let complete = closed.is_complete()
            && self
                .original
                .closed()
                .is_some_and(|closed| closed.matches(&self.original))
            && self
                .original
                .users_closed()
                .is_some_and(|closed| closed.matches(&self.original));
        report.append(closed);
        self.receiver.receive_report(&mut report)?;
        if !complete {
            return Err(retirement::pending_incomplete(
                "DNS original or transferred view is not closed",
            ));
        }
        // A borrowed physical LoanOwner lives in this adoption's original
        // control, not necessarily in `logical`. Drive it above first. Only
        // then may this Query finish waiting for that same protocol original.
        // An unhanded original still requires global Root settlement. A cached
        // original instead supplies its sealed exact receiver, which retains
        // the actual physical driver/LoanOwner without refunding its Root N or
        // substituting for this Query's own native/users/registration receipts.
        self.return_protocol_originals(end).await?;
        let capacity = self
            .capacity
            .lock()
            .expect("DNS original budget poisoned")
            .take();
        drop(capacity);
        Ok(report.finish())
    }
}

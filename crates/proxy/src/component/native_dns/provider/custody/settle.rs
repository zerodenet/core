use super::*;
impl NativeUse {
    pub(in crate::component::native_dns::provider) async fn settle(
        &self,
        end: tokio::time::Instant,
    ) -> std::io::Result<CloseReport> {
        let mut report = CloseReport::default();
        let mut logical = tokio::time::timeout_at(end, self.logical.lock())
            .await
            .map_err(super::retirement::pending_deadline)?;
        if let Some(owner) = logical.as_mut() {
            let closed = owner.close(end).await.map_err(std::io::Error::other)?;
            if !closed.is_complete() || !owner.closed().is_some_and(|closed| closed.matches(owner))
            {
                return Err(retirement::pending_incomplete(
                    "DNS protocol carrier view remains retained",
                ));
            }
            report.append(closed);
        }
        let closed = self
            .original
            .retire(end)
            .await
            .map_err(std::io::Error::other)?;
        if !closed.is_complete()
            || !self
                .original
                .closed()
                .is_some_and(|closed| closed.matches(&self.original))
            || !self
                .original
                .users_closed()
                .is_some_and(|closed| closed.matches(&self.original))
        {
            return Err(retirement::pending_incomplete(
                "DNS original or transferred view is not closed",
            ));
        }
        report.append(closed);
        self.capacity
            .lock()
            .expect("DNS original budget poisoned")
            .take();
        Ok(report)
    }
}

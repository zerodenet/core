use super::*;
use std::sync::atomic::Ordering;
impl Managed {
    pub(super) fn confirmed(&self) -> bool {
        if !self.logical.lock().unwrap().is_empty() || !self.cancel.is_cancelled() {
            return false;
        }
        let registration = self.grants.registration();
        registration
            .closed()
            .is_some_and(|closed| closed.matches(&registration))
            && registration
                .users_closed()
                .is_some_and(|closed| closed.matches(&registration))
            && self
                .current
                .lock()
                .unwrap()
                .as_ref()
                .is_none_or(|record| record.confirmed())
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.revoke(CloseReason::Shutdown);
    }
}
impl ResourceLifecycle for Owner {
    async fn start(&mut self, end: Instant) -> Result<(), BuildError> {
        if self.state.ready.load(Ordering::Acquire) && !self.state.cancel.is_cancelled() {
            return Ok(());
        }
        if self.attempted || self.state.cancel.is_cancelled() {
            return Err(error(self.state.instance, Stage::Prepare, Reason::Revoked));
        }
        self.attempted = true;
        if let Some(view) = &self.state.options.legacy_budget {
            view.activate().map_err(|source| {
                error(self.state.instance, Stage::Prepare, Reason::Capacity).with_source(source)
            })?;
        }
        #[cfg(feature = "blake3")]
        if let Some(view) = &self.state.options.udp_budget {
            view.activate().map_err(|source| {
                error(self.state.instance, Stage::Prepare, Reason::Capacity).with_source(source)
            })?;
        }

        self.state.options.pending.activate().map_err(|source| {
            error(self.state.instance, Stage::Prepare, Reason::Capacity).with_source(source)
        })?;
        self.resources.register_controlled_carrier(
            0,
            self.carrier.registration(),
            &self.state.tasks,
        )?;
        self.carrier.start(end).await?;
        self.state.ready.store(true, Ordering::Release);
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {
        self.state.cancel.cancel();
        self.carrier.revoke();
        let record = self.state.current.lock().unwrap().clone();
        if let Some(record) = record {
            record.stop();
        }
    }
    async fn shutdown(&mut self, end: Instant) -> Result<CloseReport, BuildError> {
        self.revoke(CloseReason::Shutdown);
        let mut report = self.carrier.shutdown(end).await?;
        let record = self.state.current.lock().unwrap().clone();
        if let Some(driver) = record
            .as_ref()
            .and_then(|record| record.driver.lock().unwrap().clone())
        {
            if tokio::time::timeout_at(end, driver.wait_registration_released())
                .await
                .is_err()
            {
                report.unfinished.push(
                    zero_platform_tokio::contract::lifecycle::Unfinished::Resource(
                        "same SS UDP driver Host ACK remains pending".into(),
                    ),
                );
            }
        }
        loop {
            let changed = self.state.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            self.state.logical_returned();
            if self.state.logical.lock().unwrap().is_empty() {
                break;
            }
            if tokio::time::timeout_at(end, changed).await.is_err() {
                report.unfinished.push(
                    zero_platform_tokio::contract::lifecycle::Unfinished::Resource(
                        "same SS UDP logical Tx/Rx or controls remain retained".into(),
                    ),
                );
                break;
            }
        }
        Ok(report)
    }
}

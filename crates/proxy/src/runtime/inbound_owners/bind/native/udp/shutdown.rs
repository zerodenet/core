//! Same actual Source controller branches; receipts persist before sibling faults.
use super::*;
use zero_platform_tokio::contract::{lifecycle::ResourceLifecycle, CloseReason};
impl ResourceLifecycle for DatagramResource {
    async fn start(&mut self, _: Instant) -> Result<(), ChannelError> {
        if self.0.cancel.is_cancelled() || !self.0.bound.load(Ordering::Acquire) {
            let mut error = ChannelError::new(Stage::Prepare, Reason::Revoked, Impact::Component);
            error.component = Some(self.0.identity.component);
            return Err(error);
        }
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {
        self.0.cancel.cancel();
        #[cfg(feature = "raw-ip-runtime")]
        if let Some(calls) = self.0.packet_calls.get() {
            calls.request_retirement();
        }
        #[cfg(feature = "raw-ip-runtime")]
        if let Some(reads) = &self.0.packet_reads {
            reads.revoke();
        }
        // Every native operation is synchronous. This same short lock makes
        // revoke linearize after an in-flight bind, including another thread.
        if let Some(owner) = self
            .0
            .native
            .lock()
            .expect("native datagram owner poisoned")
            .as_mut()
        {
            owner.revoke();
        }
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        #[cfg(feature = "raw-ip-runtime")]
        self.0
            .shutdown_reports
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .unfinished
            .clear();
        self.0.cancel.cancel();
        #[cfg(feature = "raw-ip-runtime")]
        if let Some(calls) = self.0.packet_calls.get() {
            calls.request_retirement();
        }
        #[cfg(feature = "raw-ip-runtime")]
        if let Some(reads) = &self.0.packet_reads {
            reads.revoke();
        }
        #[cfg(feature = "raw-ip-runtime")]
        let calls = match self.0.packet_calls.get() {
            Some(calls) => calls.shutdown(deadline).await,
            None => Ok(CloseReport::default()),
        };
        #[cfg(not(feature = "raw-ip-runtime"))]
        let calls = Ok::<_, ChannelError>(CloseReport::default());
        #[cfg(feature = "raw-ip-runtime")]
        let calls_failure = self.0.persist_shutdown_report(calls);
        #[cfg(not(feature = "raw-ip-runtime"))]
        let calls_failure = calls.err();
        let native = {
            let mut native = self
                .0
                .native
                .lock()
                .expect("native datagram owner poisoned");
            match native.as_mut() {
                Some(native) => {
                    native.revoke();
                    native.shutdown(deadline)
                }
                None => Ok(CloseReport::default()),
            }
        }; // no owner lock while retaining/forwarding foreign error payloads
        #[cfg(feature = "raw-ip-runtime")]
        {
            let native_failure = self.0.persist_shutdown_report(native).map(|source| {
                match (self.0.packet_calls.get(), self.0.packet_source_origin()) {
                    (Some(calls), Some(original)) => {
                        calls.receive_source_native_failure(&original, source)
                    }
                    _ => source,
                }
            });
            if let Some(source) = native_failure.or(calls_failure) {
                return Err(source);
            }
            return Ok(std::mem::take(
                &mut *self
                    .0
                    .shutdown_reports
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
            ));
        }
        #[cfg(not(feature = "raw-ip-runtime"))]
        {
            if let Some(source) = calls_failure {
                return Err(source);
            }
            native
        }
    }
}

#[cfg(feature = "raw-ip-runtime")]
impl NativeDatagramGrant {
    fn persist_shutdown_report(
        &self,
        report: Result<CloseReport, ChannelError>,
    ) -> Option<ChannelError> {
        match report {
            Ok(mut report) => {
                // Unfinished is a fresh observation, never a permanent debt
                // copied from a previous round. Preserve all positive receipts
                // BEFORE the independent native controller can fail/cancel.
                let unfinished = std::mem::take(&mut report.unfinished);
                self.shutdown_reports
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .append(report);
                if unfinished.is_empty() {
                    None
                } else {
                    // Incomplete must remain an ordinary positive report;
                    // restore just this round's status for its caller.
                    self.shutdown_reports
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .unfinished
                        .extend(unfinished);
                    None
                }
            }
            Err(source) => Some(source),
        }
    }
}

#[cfg(all(test, feature = "raw-ip-runtime"))]
#[path = "tests/shutdown_reports.rs"]
mod tests;

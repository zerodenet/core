//! Original driver and SDK joins precede matching native close.
use super::*;
impl PacketDeviceOwner {
    pub(super) fn evidence(&self) -> PacketDeviceEvidence {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let joined = state.driver.as_ref().is_none_or(TaskJoinWitness::is_joined);
        let mut confirmed = state.closing && !state.view_live && joined;
        #[cfg(feature = "raw-ip-runtime")]
        {
            confirmed &=
                self.memory.joined() && self.original.physical_confirmed(state.issued.as_ref());
        }
        #[cfg(not(feature = "raw-ip-runtime"))]
        {
            confirmed = false;
        }
        PacketDeviceEvidence {
            ordinal: self.ordinal,
            confirmed,
            close_fault: state.close_fault.clone(),
        }
    }
    pub(super) async fn shutdown(&self, deadline: Instant) -> CloseReport {
        #[cfg(feature = "raw-ip-runtime")]
        self.memory.stop(&self.original);
        let driver = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.closing = true;
            state.driver.clone()
        };
        if let Some(driver) = driver {
            if tokio::time::timeout_at(deadline, driver.wait())
                .await
                .is_err()
            {
                return self.unfinished("same Packet driver join is pending");
            }
        }
        // Native close follows the consumed original Scope join, never body-done.
        if self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .view_live
        {
            return self
                .unfinished("unique Packet I/O view has not returned after its driver join");
        }
        #[cfg(feature = "raw-ip-runtime")]
        {
            if !self.memory.join(deadline).await {
                return self.unfinished("same SDK reader join is pending");
            }
            let closed = tokio::time::timeout_at(deadline, self.original.close()).await;
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match closed {
                Err(_) => return self.unfinished_locked("original native Packet close is pending"),
                Ok(Err(source)) => {
                    state.close_fault.get_or_insert_with(|| {
                        Arc::new(
                            error(
                                self.identity.instance().component,
                                Reason::Io,
                                "original Packet close retained its actual source",
                            )
                            .with_source(NativeSource(source)),
                        )
                    });
                }
                Ok(Ok(receipt)) => state.receipt = receipt,
            }
            if state.receipt.is_none() {
                state.receipt = self.original.receipt();
            }
            if !self.original.physical_confirmed(state.issued.as_ref()) {
                return self.unfinished_locked("original Packet native close remains unconfirmed");
            }
            let mut report = CloseReport::default();
            if state.receipt.is_some() && !state.reported_close {
                report.closed_io.push(format!(
                    "component {} original Packet device explicitly closed",
                    self.identity.instance().component.0
                ));
                state.reported_close = true;
            }
            report
        }
        #[cfg(not(feature = "raw-ip-runtime"))]
        self.unfinished("concrete Packet provider unavailable")
    }
    fn unfinished(&self, message: &str) -> CloseReport {
        self.unfinished_locked(message)
    }
    fn unfinished_locked(&self, message: &str) -> CloseReport {
        CloseReport {
            unfinished: vec![Unfinished::Resource(format!(
                "component {} Packet device {}: {message}",
                self.identity.instance().component.0,
                self.name
            ))],
            ..Default::default()
        }
    }
}

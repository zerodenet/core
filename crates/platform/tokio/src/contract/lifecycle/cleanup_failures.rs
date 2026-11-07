//! The existing Resource's three fixed error slots; no controller/resource book.
use crate::contract::{ChannelError, CleanupFailureReceipt};
use std::{
    error::Error,
    fmt,
    sync::{Arc, OnceLock},
};
#[derive(Clone, Copy)]
pub enum CleanupFailurePhase {
    Driver,
    Shutdown,
}
#[derive(Debug, Default)]
struct Payload {
    receipt: OnceLock<Arc<ChannelError>>,
    driver: OnceLock<Arc<ChannelError>>,
    shutdown: OnceLock<Arc<ChannelError>>,
}
impl fmt::Display for Payload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("same actual resource cleanup failures")
    }
}
impl Error for Payload {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.receipt
            .get()
            .or_else(|| self.driver.get())
            .or_else(|| self.shutdown.get())
            .map(|e| e.as_ref() as &(dyn Error + 'static))
    }
}
/// Mechanical form of the existing native Resource's stable-receipt slot and
/// its separate actual driver/external-shutdown observation slots. Overflow
/// retains the incoming producer's obligation; it never acknowledges a fault
/// that this receiver did not strongly store. No IO/Scope/native proof here.
pub struct ResourceCleanupFailures {
    payload: Arc<Payload>,
    receipt: CleanupFailureReceipt,
    report: OnceLock<Arc<ChannelError>>,
}
impl Default for ResourceCleanupFailures {
    fn default() -> Self {
        let payload = Arc::new(Payload::default());
        let receipt = CleanupFailureReceipt::new();
        let erased: Arc<dyn Error + Send + Sync> = payload.clone();
        assert!(receipt.bind_payload(erased));
        Self {
            payload,
            receipt,
            report: OnceLock::new(),
        }
    }
}
impl ResourceCleanupFailures {
    /// Return false when this fixed receiver did not retain this source or its
    /// exact stable receipt payload. The original producer then keeps custody.
    pub fn try_receive(&self, actual: Arc<ChannelError>, phase: CleanupFailurePhase) -> bool {
        // receive returns an unaccepted incoming source on overflow, so its
        // return value is not custody. Read only this receiver's actual cells.
        let _ = self.receive(actual.clone(), phase);
        self.failures()
            .chain(self.report.get().cloned())
            .any(|retained| {
                Arc::ptr_eq(&retained, &actual)
                    || actual.cleanup_failure_receipt().is_some_and(|receipt| {
                        same_receipt(&retained, receipt) && receipt.is_acknowledged()
                    })
            })
    }
    pub fn receive(
        &self,
        actual: Arc<ChannelError>,
        phase: CleanupFailurePhase,
    ) -> Arc<ChannelError> {
        if actual
            .cleanup_failure_receipt()
            .is_some_and(|r| r.same_receipt(&self.receipt))
        {
            return self.report.get().cloned().unwrap_or(actual);
        }
        let phase = match phase {
            CleanupFailurePhase::Driver => &self.payload.driver,
            CleanupFailurePhase::Shutdown => &self.payload.shutdown,
        };
        let slot = if actual.cleanup_failure_receipt().is_some() {
            &self.payload.receipt
        } else {
            phase
        };
        let mut retained = slot.get_or_init(|| actual.clone()).clone();
        if let Some(receipt) = actual.cleanup_failure_receipt() {
            if !same_receipt(&retained, receipt) {
                retained = phase.get_or_init(|| actual.clone()).clone();
            }
            if same_receipt(&retained, receipt) {
                receipt.acknowledge_retained(&retained);
            } else {
                return actual;
            }
        }
        retained
    }
    pub fn receipt_failure(&self) -> Option<&Arc<ChannelError>> {
        self.payload.receipt.get()
    }
    pub fn failures(&self) -> impl Iterator<Item = Arc<ChannelError>> + '_ {
        [
            &self.payload.driver,
            &self.payload.shutdown,
            &self.payload.receipt,
        ]
        .into_iter()
        .filter_map(|s| s.get().cloned())
    }
    /// A single stable pure error payload crosses the actual owner's existing
    /// shutdown/Source/Slot fault receiver. Child errors stay strongly held.
    pub fn report(&self) -> Option<Arc<ChannelError>> {
        let first = self
            .payload
            .receipt
            .get()
            .or_else(|| self.payload.driver.get())
            .or_else(|| self.payload.shutdown.get())?;
        Some(
            self.report
                .get_or_init(|| {
                    Arc::new(
                        ChannelError::retained(first.clone())
                            .with_source(self.payload.clone())
                            .with_cleanup_failure_receipt(self.receipt.clone()),
                    )
                })
                .clone(),
        )
    }
}
fn same_receipt(source: &ChannelError, receipt: &CleanupFailureReceipt) -> bool {
    source
        .cleanup_failure_receipt()
        .is_some_and(|same| same.same_receipt(receipt))
}

#[cfg(test)]
#[path = "../../../../tests/cleanup_failures.rs"]
mod tests;

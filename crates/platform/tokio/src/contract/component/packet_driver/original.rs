use super::*;
use std::ops::Deref;
pub(super) struct PendingOriginal {
    adoption: CarrierAdoption,
    pub(super) failures: Arc<faults::PendingFailures>,
    native: AsyncMutex<()>,
    logical: AsyncMutex<()>,
    transferred: std::sync::atomic::AtomicBool,
}
impl Deref for PendingOriginal {
    type Target = CarrierAdoption;
    fn deref(&self) -> &Self::Target {
        &self.adoption
    }
}
impl PendingOriginal {
    pub(super) fn new(adoption: CarrierAdoption, record: &Record) -> Arc<Self> {
        let failures = Arc::new(faults::PendingFailures::default());
        record.failures.add_pending(failures.clone());
        Arc::new(Self {
            adoption,
            failures,
            native: AsyncMutex::new(()),
            logical: AsyncMutex::new(()),
            transferred: false.into(),
        })
    }
    pub(super) fn transfer_to_cached_driver(&self, record: &Record) -> bool {
        // Serialize with the Source's own cleanup operations. After transfer,
        // a stale one-original observer cannot call that provider again or fill
        // a fault cell after its empty Source metadata was removed.
        let Ok(_native) = self.native.try_lock() else {
            return false;
        };
        let Ok(_logical) = self.logical.try_lock() else {
            return false;
        };
        if self.transferred.load(std::sync::atomic::Ordering::Acquire) {
            return true;
        }
        if self.failures.native.failed() || self.failures.logical.failed() {
            return false;
        }
        let retained = self.adoption.retained_driver().is_some_and(|driver| {
            driver.owner().is_some_and(|owner| {
                driver.retains(&self.adoption)
                    && record.allowed.iter().any(|actual| {
                        actual.same_owner(&owner) && actual.matches_adoption(&self.adoption)
                    })
            })
        });
        if retained {
            self.transferred
                .store(true, std::sync::atomic::Ordering::Release);
        }
        retained
    }
    pub(super) fn released(&self) -> bool {
        let Ok(_native) = self.native.try_lock() else {
            return false;
        };
        let Ok(_logical) = self.logical.try_lock() else {
            return false;
        };
        self.adoption
            .closed()
            .is_some_and(|same| same.matches(&self.adoption))
            && self
                .adoption
                .users_closed()
                .is_some_and(|same| same.matches(&self.adoption))
            && self.adoption.packet_driver_registrations_released()
    }
    pub(super) fn try_native(&self, record: &Record) -> Result<CloseReport, ChannelError> {
        let Ok(_guard) = self.native.try_lock() else {
            return Ok(super::control::pending());
        };
        if self.transferred.load(std::sync::atomic::Ordering::Acquire) {
            return Ok(CloseReport::default());
        }
        if let Some(fault) = record.failed_phase(&self.failures.native) {
            return Err(fault);
        }
        record.receive_phase(
            &self.failures.native,
            self.adoption
                .try_retire_native()
                .map(|_| CloseReport::default()),
        )
    }
    pub(super) async fn close_native(
        &self,
        record: &Record,
        end: Instant,
    ) -> Result<bool, ChannelError> {
        let Ok(_guard) = tokio::time::timeout_at(end, self.native.lock()).await else {
            return Ok(false);
        };
        // A concurrent transfer may postpone this report; never invent a
        // NativeClosed receipt for the still-live canonical cached provider.
        if self.transferred.load(std::sync::atomic::Ordering::Acquire) {
            return Ok(false);
        }
        if let Some(fault) = record.failed_phase(&self.failures.native) {
            return Err(fault);
        }
        match self.adoption.retire_native(end).await {
            Ok(native) => Ok(native
                .closed()
                .is_some_and(|same| same.matches(&self.adoption))),
            Err(source) => Err(record
                .receive_phase(&self.failures.native, Err(source))
                .unwrap_err()),
        }
    }
    pub(super) fn try_logical(&self, record: &Record) -> Result<CloseReport, ChannelError> {
        let Ok(_guard) = self.logical.try_lock() else {
            return Ok(super::control::pending());
        };
        if self.transferred.load(std::sync::atomic::Ordering::Acquire) {
            return Ok(super::control::pending());
        }
        if let Some(fault) = record.failed_phase(&self.failures.logical) {
            return Err(fault);
        }
        record.receive_phase(&self.failures.logical, self.adoption.try_retire())
    }
    pub(super) async fn close_logical(
        &self,
        record: &Record,
        end: Instant,
    ) -> Result<CloseReport, ChannelError> {
        let Ok(_guard) = tokio::time::timeout_at(end, self.logical.lock()).await else {
            return Ok(super::control::pending());
        };
        if self.transferred.load(std::sync::atomic::Ordering::Acquire) {
            return Ok(super::control::pending());
        }
        if let Some(fault) = record.failed_phase(&self.failures.logical) {
            return Err(fault);
        }
        record.receive_phase(&self.failures.logical, self.adoption.retire(end).await)
    }
}

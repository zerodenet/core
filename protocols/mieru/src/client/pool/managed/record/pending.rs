//! Same actual Mieru carrier record, original setup and selected child ledger.
use super::*;
#[derive(Clone)]
pub(crate) struct PendingCarriers {
    pub(super) record: Arc<CarrierRecord>,
    pub(super) resources: std::sync::Weak<dyn zero_platform_tokio::contract::component::ResourceAccess>,
}
impl PendingCarriers {
    pub(crate) fn prepare_protocols(
        &self,
    ) -> Result<(), zero_platform_tokio::contract::ChannelError> {
        use zero_platform_tokio::contract::{ChannelError, Impact, Reason, Stage};
        let fail = || ChannelError::new(Stage::Prepare, Reason::StalePlan, Impact::Component);
        let original = self.record.adoption().ok_or_else(fail)?;
        if self.record.2 == 0 || !original.has_original_root() {
            return Ok(());
        }
        if self.record.3.get().is_none() {
            let memory = self
                .resources
                .upgrade()
                .ok_or_else(fail)?
                .packet_driver_metadata()?;
            let _ = self
                .record
                .3
                .set(crate::client::pool::managed::protocols::Children::new(
                    memory,
                    self.record.2,
                ));
        }
        Ok(())
    }
    pub(crate) fn current_receiver(&self, setup: &TaskJoinWitness) -> Option<TaskJoinWitness> {
        let original = self.record.adoption()?;
        let native = tokio::task::try_id();
        if original.matches_auxiliary(setup)
            && !setup.is_joined()
            && setup.native_task_id().is_some_and(|id| Some(id) == native)
        {
            return Some(setup.clone());
        }
        let retained = original.retained_driver()?;
        let driver = retained.driver();
        (retained.retains(&original)
            && retained
                .adoption()
                .is_some_and(|actual| actual.same_slot(&original))
            && !driver.is_joined()
            && driver.native_task_id().is_some_and(|id| Some(id) == native))
        .then(|| driver.clone())
    }
    pub(crate) fn protocols(&self) -> Option<&crate::client::pool::managed::protocols::Children> {
        self.record.protocols()
    }

    pub(crate) fn bind_cleanup_provider(
        &self,
        provider: &zero_platform_tokio::contract::network::UpstreamTransport,
        binding: &zero_platform_tokio::contract::network::UpstreamBinding,
    ) -> Result<(), zero_platform_tokio::contract::ChannelError> {
        let mut record = self.record.0.lock().unwrap();
        if record
            .cleanup
            .as_ref()
            .is_some_and(|(_, original)| original != binding)
        {
            return Err(zero_platform_tokio::contract::ChannelError::new(
                zero_platform_tokio::contract::Stage::Prepare,
                zero_platform_tokio::contract::Reason::StalePlan,
                zero_platform_tokio::contract::Impact::Component,
            ));
        }
        if record.cleanup.is_none() {
            record.cleanup = Some((provider.clone(), binding.clone()));
        }
        Ok(())
    }
    pub(crate) fn capacity(&self) -> Option<usize> {
        let record = self.record.0.lock().unwrap();
        (!record.finished && record.attempted && record.adoption.is_some()).then_some(self.record.2)
    }
    /// Actual foreign cleanup authority is bounded by this selected prefix's
    /// role count. It never substitutes for this pool record's root original.
    pub(crate) fn retain_pending(
        &self,
        adoption: CarrierAdoption,
    ) -> Result<(), zero_platform_tokio::contract::ChannelError> {
        let mut record = self.record.0.lock().unwrap();
        if record
            .pending
            .iter()
            .any(|prior| prior.same_slot(&adoption))
        {
            return Ok(());
        }
        let reason = if record.finished || !record.attempted || record.adoption.is_none() {
            Some(zero_platform_tokio::contract::Reason::StalePlan)
        } else if record.pending.len() >= self.record.2 {
            Some(zero_platform_tokio::contract::Reason::Capacity)
        } else {
            None
        };
        if let Some(reason) = reason {
            return Err(zero_platform_tokio::contract::ChannelError::new(
                zero_platform_tokio::contract::Stage::Prepare,
                reason,
                zero_platform_tokio::contract::Impact::Component,
            ));
        }
        record.pending.push(adoption);
        Ok(())
    }
}

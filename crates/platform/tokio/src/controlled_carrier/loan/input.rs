//! Retain the actual input controller when standard framing wraps its loan.
//! The output grants no native close or Scope evidence for that input.
use super::*;
use crate::contract::{ChannelError, Impact, Reason, Stage};
use crate::controlled_carrier::CarrierAdoption;
use std::sync::Weak;

pub(super) struct Input {
    // Same original allocation, erased only in passive custody storage. The
    // actual framing Control still executes cleanup through its typed owner.
    pub(super) _owner: Arc<dyn std::any::Any + Send + Sync>,
    native: Weak<dyn std::any::Any + Send + Sync>,
    pub(super) alive: Weak<State>,
}
impl Input {
    pub(super) fn native(&self) -> Option<CarrierAdoption> {
        Some(CarrierAdoption {
            slot: self.native.upgrade()?.downcast().ok()?,
        })
    }
}
impl LoanOwner {
    /// The same persistent standard framing slot supplies its actual original
    /// LoanOwner, not a lineage declaration or callback. Strongly retain that
    /// exact slot before returning the outer loan to a cached driver.
    pub async fn retain_input_owner(
        &mut self,
        input: Arc<tokio::sync::Mutex<Option<LoanOwner>>>,
    ) -> Result<(), ChannelError> {
        let fail = || ChannelError::new(Stage::Prepare, Reason::StalePlan, Impact::Component);
        if self.input.is_some() || self.native_control.is_some() || self.closed {
            return Err(fail());
        }
        let (native, alive) = {
            let guard = input.lock().await;
            let original = guard.as_ref().ok_or_else(fail)?;
            if !self.original().same_original(original.original())
                || original.closed
                || !original
                    .state
                    .control_alive
                    .load(std::sync::atomic::Ordering::Acquire)
            {
                return Err(fail());
            }
            let native = original
                .native_control
                .clone()
                .or_else(|| original.input.as_ref().and_then(Input::native))
                .ok_or_else(fail)?;
            (native, Arc::downgrade(&original.state))
        };
        self.input = Some(Input {
            _owner: input,
            native: Arc::<crate::controlled_carrier::slot::Slot>::downgrade(&native.slot),
            alive,
        });
        Ok(())
    }
}

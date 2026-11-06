//! A reused actual slot cannot change the kernel accounting roots.
use super::*;
impl ComponentSlot {
    pub(in crate::component) fn original_budgets_for_start(
        &self,
    ) -> Result<Arc<crate::HostOriginalBudgets>, ChannelError> {
        if let Some(originals) = self.originals.get() {
            return Ok(originals.clone());
        }
        if self.admission.get().is_some() {
            return Err(self.failure(Reason::Internal, "actual host original roots not installed"));
        }
        // An explicitly independent library slot owns its own lifecycle. A
        // hosted production slot never manufactures a replacement root here.
        Ok(self.originals.get_or_init(Arc::default).clone())
    }
    pub(in crate::component) fn bound_original_budgets(
        &self,
    ) -> Option<Arc<crate::HostOriginalBudgets>> {
        self.originals.get().cloned()
    }
    pub(in crate::component) fn bind_original_budgets(
        &self,
        originals: Arc<crate::HostOriginalBudgets>,
    ) {
        if let Err(originals) = self.originals.set(originals) {
            assert!(
                Arc::ptr_eq(self.originals.get().expect("same slot roots"), &originals),
                "reused slot cannot cross kernel quota roots"
            );
        }
    }
}

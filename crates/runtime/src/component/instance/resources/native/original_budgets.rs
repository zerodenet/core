//! Original-provider construction data; never exposed by ResourceAccess.
use super::*;
impl HostResources {
    pub fn original_budgets(&self) -> &Arc<crate::HostOriginalBudgets> {
        &self.originals
    }
    pub fn original_tasks(
        &self,
    ) -> Option<&zero_platform_tokio::contract::scope::WeakTaskRegistrar> {
        self.original_tasks.as_ref()
    }
}

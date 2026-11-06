//! One root attachment shared by all plans of the existing catalog.
use super::*;
impl ComponentCatalog {
    pub(crate) fn bind_original_budgets(&self, originals: Arc<crate::HostOriginalBudgets>) {
        if let Err(originals) = self.originals.set(originals) {
            assert!(
                Arc::ptr_eq(
                    self.originals.get().expect("same catalog roots"),
                    &originals
                ),
                "one concrete kernel root per catalog"
            );
        }
    }
    pub(super) fn original_budgets(&self) -> Arc<crate::HostOriginalBudgets> {
        // Public standalone catalogs own one explicit lifetime; RuntimeHost always injects before prepare.
        self.originals.get_or_init(Arc::default).clone()
    }
}

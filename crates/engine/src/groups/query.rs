//! Borrow the actual state maps while one synchronous query is prepared.
use super::*;
use std::sync::MutexGuard;
pub(crate) struct GroupQueryBorrow<'a> {
    selector: MutexGuard<'a, HashMap<TargetId, SelectorState>>,
    urltest: MutexGuard<'a, HashMap<TargetId, UrlTestState>>,
}
impl OutboundGroupStateStore {
    pub(crate) fn query_borrow(&self) -> GroupQueryBorrow<'_> {
        GroupQueryBorrow {
            selector: self
                .selector
                .lock()
                .expect("selector group state lock poisoned"),
            urltest: self
                .urltest
                .lock()
                .expect("urltest group state lock poisoned"),
        }
    }
}
impl GroupQueryBorrow<'_> {
    pub(crate) fn selector(&self, id: TargetId) -> Option<TargetId> {
        self.selector.get(&id).map(|state| state.selected)
    }
    pub(crate) fn urltest(&self, id: TargetId) -> Option<&UrlTestGroupState> {
        self.urltest.get(&id).map(|state| &state.state)
    }
}

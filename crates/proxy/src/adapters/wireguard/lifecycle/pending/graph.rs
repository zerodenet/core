//! Erase only the existing weak graph back-reference. The same allocation is
//! recovered on lookup; no graph, owner, provider or configuration is copied.
use super::super::recipe::component::LinkedStackOptions;
use std::sync::{Arc, Weak};

trait ActualGraph: Send + Sync {
    fn actual(self: Arc<Self>) -> Arc<LinkedStackOptions>;
}
impl ActualGraph for LinkedStackOptions {
    fn actual(self: Arc<Self>) -> Arc<LinkedStackOptions> {
        self
    }
}
#[derive(Default)]
pub(super) struct WeakGraph(Option<Weak<dyn ActualGraph>>);
impl WeakGraph {
    pub(super) fn new(graph: &Arc<LinkedStackOptions>) -> Self {
        let actual = Arc::downgrade(graph);
        let actual: Weak<dyn ActualGraph> = actual;
        Self(Some(actual))
    }
    pub(super) fn upgrade(&self) -> Option<Arc<LinkedStackOptions>> {
        self.0.as_ref()?.upgrade().map(ActualGraph::actual)
    }
}

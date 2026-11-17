//! Cache only the same weak graph allocation, without expanding auto traits
//! through graph -> outer recipe -> inventory -> adapter -> this cache again.
use super::StandaloneStackOptions;
use std::sync::{Arc, Weak};
trait ActualGraph: Send + Sync {
    fn actual(self: Arc<Self>) -> Arc<StandaloneStackOptions>;
}
impl ActualGraph for StandaloneStackOptions {
    fn actual(self: Arc<Self>) -> Arc<Self> {
        self
    }
}
#[derive(Clone)]
pub(in crate::adapters::wireguard) struct WeakStandaloneGraph(Weak<dyn ActualGraph>);
impl WeakStandaloneGraph {
    pub(in crate::adapters::wireguard) fn new(graph: &Arc<StandaloneStackOptions>) -> Self {
        let actual = Arc::downgrade(graph);
        Self(actual)
    }
    pub(in crate::adapters::wireguard) fn upgrade(&self) -> Option<Arc<StandaloneStackOptions>> {
        self.0.upgrade().map(ActualGraph::actual)
    }
}

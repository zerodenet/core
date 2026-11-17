//! Exact weak owner allocation without recursively expanding pool auto traits
//! through cached client -> physical -> owner -> cached client again.
use super::Managed;
use std::sync::{Arc, Weak};
trait ActualManaged: Send + Sync {
    fn actual(self: Arc<Self>) -> Arc<Managed>;
}
impl ActualManaged for Managed {
    fn actual(self: Arc<Self>) -> Arc<Self> {
        self
    }
}
pub(in crate::grpc::owned) struct WeakManaged(Weak<dyn ActualManaged>);
impl WeakManaged {
    pub(in crate::grpc::owned) fn new(owner: &Arc<Managed>) -> Self {
        let actual = Arc::downgrade(owner);
        Self(actual)
    }
    pub(in crate::grpc::owned) fn upgrade(&self) -> Option<Arc<Managed>> {
        self.0.upgrade().map(ActualManaged::actual)
    }
}

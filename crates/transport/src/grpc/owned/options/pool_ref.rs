//! Same pool allocation, with a private nonrecursive cold-reference shape.
//! No IO/driver method or new pool is exposed through this erased pointer.
use crate::grpc::pool::State;
use std::{
    any::Any,
    sync::{Arc, Weak},
};
#[derive(Clone)]
pub(in crate::grpc::owned) struct PoolRef(Weak<dyn Any + Send + Sync>);
impl PoolRef {
    pub(super) fn new(actual: &Arc<State>) -> Self {
        // Unsizing and downgrading preserve this exact allocation/header.
        // `actual` remains strong through the whole conversion.
        let erased: Arc<dyn Any + Send + Sync> = actual.clone();
        Self(Arc::downgrade(&erased))
    }
    pub(in crate::grpc::owned) fn upgrade(&self) -> Option<Arc<State>> {
        // Only new() constructs this private reference. Downcast consumes the
        // same upgraded Arc; it neither allocates nor chooses another pool.
        self.0.upgrade()?.downcast::<State>().ok()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn erased_recipe_reference_preserves_the_exact_original_pool_header() {
        let first = crate::grpc::GrpcPool::default();
        let second = crate::grpc::GrpcPool::default();
        let reference = PoolRef::new(&first.0);
        assert_eq!(Arc::strong_count(&first.0), 1);
        assert_eq!(Arc::weak_count(&first.0), 1);
        let observed = reference
            .upgrade()
            .expect("same actual allocation remains live");
        assert!(Arc::ptr_eq(&first.0, &observed));
        assert!(!Arc::ptr_eq(&second.0, &observed));
        assert_eq!(Arc::strong_count(&first.0), 2);
        drop(first);
        let again = reference.upgrade().unwrap();
        assert!(Arc::ptr_eq(&observed, &again));
        drop(again);
        drop(observed);
        // This is only Weak liveness, never a native/Scope/cleanup receipt.
        assert!(reference.upgrade().is_none());
    }
}

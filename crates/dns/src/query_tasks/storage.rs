//! Erase only the existing Shared allocation, never a task/future/output.
//! The constructor is the sole coercion: real Shared must prove Send + Sync.
use super::Shared;
use std::{
    any::Any,
    fmt,
    ops::Deref,
    sync::{Arc, Weak},
};
type Erased = dyn Any + Send + Sync;
pub(super) type WeakStorage = Weak<Erased>;
#[derive(Clone)]
pub(super) struct Storage(Arc<Erased>);
impl Storage {
    pub(super) fn new(shared: Shared) -> Self {
        // Unsizing this exact original Arc allocates no wrapper/header/book.
        // Its concrete owner and all original fields retain their lifecycle.
        Self(Arc::new(shared))
    }
    pub(super) fn downgrade(&self) -> WeakStorage {
        Arc::downgrade(&self.0)
    }
    pub(super) fn same(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    pub(super) fn same_arc(&self, other: &Arc<Erased>) -> bool {
        Arc::ptr_eq(&self.0, other)
    }
}
impl Deref for Storage {
    type Target = Shared;
    fn deref(&self) -> &Shared {
        self.0
            .downcast_ref::<Shared>()
            .expect("sealed original DNS task Shared allocation")
    }
}
impl fmt::Debug for Storage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DnsQueryStorage").finish_non_exhaustive()
    }
}

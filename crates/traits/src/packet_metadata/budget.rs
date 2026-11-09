//! Same-counter consumer view. The actual Host retains configuration authority.
use super::*;
/// Allocation, snapshot and identity only. Cloning does not create a book.
/// The raw Counter, limit setters and candidate/issuer factories are not exposed.
///
/// ```compile_fail
/// use zero_traits::packet_metadata::{PacketMetadataBudget, PacketMetadataLayout};
/// fn cannot_configure(budget: PacketMetadataBudget) {
///     budget.install_limit(PacketMetadataLayout::new(1, 4096)).unwrap();
/// }
/// ```
/// ```compile_fail
/// use zero_traits::packet_metadata::{PacketMetadataBudget, PacketMetadataLayout, PacketMetadataCredit};
/// fn cannot_issue(budget: PacketMetadataBudget, paid: PacketMetadataCredit) {
///     let candidate = budget.prepare_limit(PacketMetadataLayout::new(1, 4096), paid);
/// }
/// ```
#[derive(Clone)]
pub struct PacketMetadataBudget {
    pub(super) counter: Arc<PacketMetadataCounter>,
}
impl PacketMetadataBudget {
    /// Actual Host construction, from the already-owned original Counter.
    /// This moves only the existing Arc; it does not allocate or change policy.
    pub fn from_counter(counter: Arc<PacketMetadataCounter>) -> Self {
        Self { counter }
    }
    pub fn snapshot(&self) -> PacketMetadataSnapshot {
        self.counter.snapshot()
    }
    pub fn reserve(
        &self,
        layout: PacketMetadataLayout,
        wake: Waker,
    ) -> Result<PacketMetadataCredit, PacketMemoryError> {
        self.counter.reserve(layout, wake)
    }
    pub fn same_budget(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.counter, &other.counter)
    }
}
impl fmt::Debug for PacketMetadataBudget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PacketMetadataBudget")
            .field("snapshot", &self.snapshot())
            .finish()
    }
}

use std::any::TypeId;
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct ManagedUdpConnectionCacheKey {
    owner: TypeId,
    value: String,
}
impl ManagedUdpConnectionCacheKey {
    pub(super) fn new<T: 'static>(value: impl Into<String>) -> Self {
        Self {
            owner: TypeId::of::<T>(),
            value: value.into(),
        }
    }
}

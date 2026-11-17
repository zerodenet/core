//! Same weak Record allocation and prepaid header, without recursively
//! expanding Record -> loan/stream -> Record auto traits in every consumer.
use super::{Record, RecordRef};
use std::any::Any;
#[cfg(feature = "native-packet-origin")]
type ActualWeak = zero_traits::packet_metadata::PacketMetadataWeak<dyn Any + Send + Sync>;
#[cfg(not(feature = "native-packet-origin"))]
type ActualWeak = std::sync::Weak<dyn Any + Send + Sync>;
#[derive(Clone)]
pub(super) struct RecordWeak(ActualWeak);
impl RecordWeak {
    pub(super) fn new(record: &RecordRef) -> Self {
        #[cfg(feature = "native-packet-origin")]
        {
            let erased = record.clone().into_any();
            Self(zero_traits::packet_metadata::PacketMetadataArc::downgrade(
                &erased,
            ))
        }
        #[cfg(not(feature = "native-packet-origin"))]
        {
            let erased: std::sync::Arc<dyn Any + Send + Sync> = record.clone();
            Self(std::sync::Arc::downgrade(&erased))
        }
    }
    pub(super) fn upgrade(&self) -> Option<RecordRef> {
        self.0.upgrade()?.downcast::<Record>().ok()
    }
    pub(super) fn ptr_eq(left: &Self, right: &Self) -> bool {
        ActualWeak::ptr_eq(&left.0, &right.0)
    }
}

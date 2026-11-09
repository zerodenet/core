//! Closed hosted strong/weak storage; standalone keeps its original ownership.
use super::*;
use zero_traits::packet_metadata::{PacketMetadataArc, PacketMetadataWeak};
#[derive(Clone)]
pub(crate) struct PendingOpenRef(Strong);
#[derive(Clone)]
enum Strong {
    Hosted(PacketMetadataArc<PendingOpen>),
    Library(Arc<PendingOpen>),
}
#[derive(Clone)]
pub(crate) struct PendingOpenWeak(WeakStorage);
#[derive(Clone)]
enum WeakStorage {
    Hosted(PacketMetadataWeak<PendingOpen>),
    Library(Weak<PendingOpen>),
}
impl PendingOpenRef {
    pub(super) fn new(
        value: PendingOpen,
        memory: Option<&zero_platform_tokio::contract::component::PacketDriverMetadataView>,
    ) -> Result<Self, ChannelError> {
        match memory {
            Some(memory) => {
                let credit = memory
                    .reserve(
                        PacketMetadataArc::<PendingOpen>::layout().map_err(|source| {
                            admission_error(Reason::Capacity).with_source(source)
                        })?,
                    )
                    .map_err(|source| admission_error(Reason::Capacity).with_source(source))?;
                PacketMetadataArc::from_prepaid(value, credit)
                    .map(|actual| Self(Strong::Hosted(actual)))
                    .map_err(|(source, _, _)| admission_error(Reason::Capacity).with_source(source))
            }
            None => Ok(Self(Strong::Library(Arc::new(value)))),
        }
    }
    pub(super) fn downgrade(&self) -> PendingOpenWeak {
        PendingOpenWeak(match &self.0 {
            Strong::Hosted(value) => WeakStorage::Hosted(PacketMetadataArc::downgrade(value)),
            Strong::Library(value) => WeakStorage::Library(Arc::downgrade(value)),
        })
    }
}
impl std::ops::Deref for PendingOpenRef {
    type Target = PendingOpen;
    fn deref(&self) -> &PendingOpen {
        match &self.0 {
            Strong::Hosted(value) => value,
            Strong::Library(value) => value,
        }
    }
}
impl PendingOpenWeak {
    pub(crate) fn upgrade(&self) -> Option<PendingOpenRef> {
        match &self.0 {
            WeakStorage::Hosted(value) => value
                .upgrade()
                .map(|value| PendingOpenRef(Strong::Hosted(value))),
            WeakStorage::Library(value) => value
                .upgrade()
                .map(|value| PendingOpenRef(Strong::Library(value))),
        }
    }
}

#[derive(Clone)]
pub(super) enum ProtocolControlRef {
    Hosted(
        PacketMetadataArc<AsyncMutex<zero_platform_tokio::contract::lifecycle::BoxChannelControl>>,
    ),
    Library(Arc<AsyncMutex<zero_platform_tokio::contract::lifecycle::BoxChannelControl>>),
}
impl ProtocolControlRef {
    pub(super) fn new(
        value: zero_platform_tokio::contract::lifecycle::BoxChannelControl,
        credit: Option<zero_traits::packet_metadata::PacketMetadataCredit>,
    ) -> Self {
        let value = AsyncMutex::new(value);
        match credit {
            Some(credit) => Self::Hosted(
                PacketMetadataArc::from_prepaid(value, credit).unwrap_or_else(|_| {
                    unreachable!("same protocol control layout prepaid before first IO")
                }),
            ),
            None => Self::Library(Arc::new(value)),
        }
    }
}
impl std::ops::Deref for ProtocolControlRef {
    type Target = AsyncMutex<zero_platform_tokio::contract::lifecycle::BoxChannelControl>;
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Hosted(value) => value,
            Self::Library(value) => value,
        }
    }
}

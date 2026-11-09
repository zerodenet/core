//! Closed ownership of the existing logical lifetime allocation.
use super::Lifetime;
use std::{ops::Deref, sync::Arc};
#[derive(Clone)]
pub(in crate::split_http) enum LifetimeRef {
    Library(Arc<Lifetime>),
    #[cfg(feature = "quic")]
    Hosted(zero_traits::packet_metadata::PacketMetadataArc<Lifetime>),
}
impl Default for LifetimeRef {
    fn default() -> Self {
        Self::Library(Arc::new(Lifetime::default()))
    }
}
impl Deref for LifetimeRef {
    type Target = Lifetime;
    fn deref(&self) -> &Lifetime {
        match self {
            Self::Library(original) => original,
            #[cfg(feature = "quic")]
            Self::Hosted(original) => original,
        }
    }
}
impl LifetimeRef {
    pub(in crate::split_http) fn as_ptr(&self) -> *const Lifetime {
        &**self
    }
    #[cfg(feature = "quic")]
    pub(in crate::split_http) fn prepare(
        memory: &zero_platform_tokio::contract::component::PacketDriverMetadataView,
    ) -> std::io::Result<Self> {
        use std::io;
        use zero_traits::packet_metadata::PacketMetadataArc;
        let credit = memory
            .reserve(PacketMetadataArc::<Lifetime>::layout().map_err(io::Error::other)?)
            .map_err(io::Error::other)?;
        PacketMetadataArc::from_prepaid(Lifetime::default(), credit)
            .map(Self::Hosted)
            .map_err(|(source, _, _)| io::Error::other(source))
    }
}

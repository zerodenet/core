#[cfg(feature = "managed-datagram-runtime")]
mod datagram;
mod key;
#[cfg(feature = "managed-datagram-runtime")]
pub(crate) use datagram::ManagedDatagramConnectionCache;

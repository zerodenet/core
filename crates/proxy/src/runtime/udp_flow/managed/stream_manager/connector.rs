mod flow;
#[cfg(feature = "managed-stream-runtime")]
mod packet;
#[cfg(feature = "managed-stream-runtime")]
mod tuple;

pub(crate) use flow::{ManagedStreamConnectorFlow, ManagedStreamFlowConnector};
pub(crate) use packet::ManagedPacketUdpResumeConnector;
pub(crate) use tuple::ManagedTupleUdpResumeConnector;

#[derive(Debug, Clone)]
pub(crate) struct ManagedTupleUdpResume<T>(pub(crate) T);

impl<T> ManagedTupleUdpResume<T> {
    pub(crate) fn new(inner: T) -> Self {
        Self(inner)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ManagedPacketUdpResume<T>(pub(crate) T);

impl<T> ManagedPacketUdpResume<T> {
    pub(crate) fn new(inner: T) -> Self {
        Self(inner)
    }
}

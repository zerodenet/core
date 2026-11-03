use crate::runtime::tcp_dispatch::operation::LazyTcpRelayCarrier;
use crate::transport::RelayCarrier;

pub(crate) enum ManagedRelayStreamCarrier<'a> {
    Ready(RelayCarrier),
    Lazy(Box<LazyTcpRelayCarrier<'a>>),
}

impl From<RelayCarrier> for ManagedRelayStreamCarrier<'_> {
    fn from(carrier: RelayCarrier) -> Self {
        Self::Ready(carrier)
    }
}

impl<'a> From<LazyTcpRelayCarrier<'a>> for ManagedRelayStreamCarrier<'a> {
    fn from(carrier: LazyTcpRelayCarrier<'a>) -> Self {
        Self::Lazy(Box::new(carrier))
    }
}

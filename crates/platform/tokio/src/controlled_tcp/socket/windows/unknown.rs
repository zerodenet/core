//! A consumed registration is a permanent unknown outcome, not a live SOCKET.
use std::{io, os::windows::io::RawSocket, sync::Arc};
#[derive(Clone, Debug)]
pub(super) struct ConsumedUnknown {
    identity: RawSocket,
    source: Arc<io::Error>,
}
impl ConsumedUnknown {
    pub(super) fn new(identity: RawSocket, source: io::Error) -> Self {
        Self {
            identity,
            source: Arc::new(source),
        }
    }
    pub(super) fn error(&self) -> io::Error {
        io::Error::new(self.source.kind(), self.clone())
    }
}
impl std::fmt::Display for ConsumedUnknown {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "TCP registration consumed original SOCKET {:?}; native close outcome is unknown: {}",
            self.identity, self.source
        )
    }
}
impl std::error::Error for ConsumedUnknown {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}

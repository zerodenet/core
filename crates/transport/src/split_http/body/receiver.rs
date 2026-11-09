//! Compatibility FIFO or same-driver paid body FIFO; each frame stays owned.
use super::*;
pub(in crate::split_http) enum Receiver {
    Legacy(mpsc::Receiver<io::Result<Bytes>>),
    Owned(super::super::xmux::owned::pipe::Receiver),
}
impl From<mpsc::Receiver<io::Result<Bytes>>> for Receiver {
    fn from(value: mpsc::Receiver<io::Result<Bytes>>) -> Self {
        Self::Legacy(value)
    }
}
impl From<super::super::xmux::owned::pipe::Receiver> for Receiver {
    fn from(value: super::super::xmux::owned::pipe::Receiver) -> Self {
        Self::Owned(value)
    }
}
impl Receiver {
    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<io::Result<Bytes>>> {
        match self {
            Self::Legacy(inner) => inner.poll_recv(cx),
            Self::Owned(inner) => inner.poll_recv(cx),
        }
    }
    pub fn is_closed(&self) -> bool {
        match self {
            Self::Legacy(inner) => inner.is_closed(),
            Self::Owned(inner) => inner.is_closed(),
        }
    }
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Legacy(inner) => inner.is_empty(),
            Self::Owned(inner) => inner.is_empty(),
        }
    }
}

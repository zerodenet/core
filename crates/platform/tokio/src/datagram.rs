//! Native datagram polling for owners that multiplex one receive task.
use std::{
    io,
    net::SocketAddr,
    task::{Context, Poll},
};
use tokio::io::ReadBuf;

impl crate::TokioDatagramSocket {
    /// Receive into caller-owned storage without constructing a waiting task.
    ///
    /// As with Tokio's native poll API, only the most recent receive waker is
    /// retained. The caller must serialize receive polling for this socket.
    /// Pending readiness is not an I/O error or a received datagram.
    pub fn poll_recv_from_addr(
        &self,
        cx: &mut Context<'_>,
        bytes: &mut [u8],
    ) -> Poll<io::Result<(usize, SocketAddr)>> {
        let mut buffer = ReadBuf::new(bytes);
        match self.inner.poll_recv_from(cx, &mut buffer) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                if let Some(observer) = &self.observer {
                    match &result {
                        Ok(_) => observer.received_datagram(buffer.filled().len()),
                        Err(_) => observer.error(),
                    }
                }
                Poll::Ready(result.map(|peer| (buffer.filled().len(), peer)))
            }
        }
    }
}

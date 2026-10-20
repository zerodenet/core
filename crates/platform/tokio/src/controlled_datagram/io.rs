use super::*;
use std::{
    future::poll_fn,
    net::SocketAddr,
    task::{Context, Poll},
};
impl ControlledDatagramSocket {
    pub(super) fn prepare_poll(
        &self,
        direction: super::wake_relay::Direction,
        cx: &Context<'_>,
    ) -> std_io::Result<super::wake_relay::PreparedPoll> {
        self.entry
            .wake_relay
            .prepare(direction, cx)
            .map_err(|error| {
                // Retirement must not erase an existing packet-policy or native
                // Unknown source. This runs after caller clone/drop completed.
                let packet = self
                    .entry
                    .packet_failure
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone();
                if let Some(source) = packet {
                    return std_io::Error::other(source);
                }
                match &*self.entry.lock() {
                    NativeHandleState::Unknown(source) => std_io::Error::other(source.clone()),
                    _ => error,
                }
            })
    }
    pub fn reservation(&self) -> DatagramReservationId {
        self.entry.id
    }
    pub fn egress_interface(&self) -> Option<&EgressInterface> {
        self.interface.as_ref()
    }
    pub fn local_addr(&self) -> std_io::Result<SocketAddr> {
        match &*self.entry.lock() {
            NativeHandleState::Live(socket) => socket.local_addr(),
            NativeHandleState::Raw(socket) => socket.local_addr(),
            other => Err(unavailable(other)),
        }
    }
    /// In-place observation keeps the original in the persistent owner even
    /// if an external boundary observer panics.
    pub fn set_observer(
        &mut self,
        observer: Option<Arc<dyn zero_traits::IoObserver>>,
    ) -> std_io::Result<()> {
        let old = self.replace_observer(observer.clone())?;
        super::observation::boundary(&observer, old);
        Ok(())
    }
    pub(super) fn replace_observer(
        &self,
        observer: super::observation::Observer,
    ) -> std_io::Result<super::observation::Observer> {
        match &mut *self.entry.lock() {
            NativeHandleState::Live(socket) if !self.entry.retired.is_cancelled() => {
                Ok(std::mem::replace(&mut socket.observer, observer))
            }
            other => Err(unavailable(other)),
        }
    }
    pub fn poll_send_to_addr(
        &self,
        cx: &mut Context<'_>,
        bytes: &[u8],
        peer: SocketAddr,
    ) -> Poll<std_io::Result<usize>> {
        let mut prepared = match self.prepare_poll(super::wake_relay::Direction::Write, cx) {
            Ok(prepared) => prepared,
            Err(error) => return Poll::Ready(Err(error)),
        };
        let (result, observer) = self.poll_send_unobserved(&prepared, bytes, peer);
        super::observation::send(observer, &result);
        prepared.pending(result.is_pending());
        result
    }
    pub(super) fn poll_send_unobserved(
        &self,
        prepared: &super::wake_relay::PreparedPoll,
        bytes: &[u8],
        peer: SocketAddr,
    ) -> (Poll<std_io::Result<usize>>, super::observation::Observer) {
        match &*self.entry.lock() {
            NativeHandleState::Live(socket) if !self.entry.retired.is_cancelled() => (
                socket
                    .inner
                    .poll_send_to(&mut prepared.context(), bytes, peer),
                socket.observer.clone(),
            ),
            other => (Poll::Ready(Err(unavailable(other))), None),
        }
    }
    pub fn poll_recv_from_addr(
        &self,
        cx: &mut Context<'_>,
        bytes: &mut [u8],
    ) -> Poll<std_io::Result<(usize, SocketAddr)>> {
        let mut prepared = match self.prepare_poll(super::wake_relay::Direction::Read, cx) {
            Ok(prepared) => prepared,
            Err(error) => return Poll::Ready(Err(error)),
        };
        let (result, observer) = self.poll_recv_unobserved(&prepared, bytes);
        super::observation::receive(observer, &result);
        prepared.pending(result.is_pending());
        result
    }
    pub(super) fn poll_recv_unobserved(
        &self,
        prepared: &super::wake_relay::PreparedPoll,
        bytes: &mut [u8],
    ) -> (
        super::observation::ReceivePoll,
        super::observation::Observer,
    ) {
        match &*self.entry.lock() {
            NativeHandleState::Live(socket) if !self.entry.retired.is_cancelled() => {
                let mut buffer = tokio::io::ReadBuf::new(bytes);
                let result = socket
                    .inner
                    .poll_recv_from(&mut prepared.context(), &mut buffer)
                    .map(|result| result.map(|peer| (buffer.filled().len(), peer)));
                (result, socket.observer.clone())
            }
            other => (Poll::Ready(Err(unavailable(other))), None),
        }
    }
    pub async fn send_to_addr(&self, bytes: &[u8], peer: SocketAddr) -> std_io::Result<usize> {
        poll_fn(|cx| self.poll_send_to_addr(cx, bytes, peer)).await
    }
    pub async fn recv_from_addr(&self, bytes: &mut [u8]) -> std_io::Result<(usize, SocketAddr)> {
        poll_fn(|cx| self.poll_recv_from_addr(cx, bytes)).await
    }
    /// The sole I/O owner invokes this after retiring its old poll/mappings.
    /// The actual result is retained before returning a reusable-slot receipt.
    pub fn close(&self) -> Result<DatagramCloseReceipt, Arc<std_io::Error>> {
        self.entry.close()
    }
}
pub(super) fn unavailable(native: &NativeHandleState) -> std_io::Error {
    match native {
        NativeHandleState::Unknown(source) => std_io::Error::other(source.clone()),
        _ => std_io::Error::new(
            std_io::ErrorKind::NotConnected,
            "original UDP socket is not live",
        ),
    }
}

//! Same target FIFO as the original managed cache, with cancellation ownership.
use super::*;
use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore};
pub(super) struct Pending {
    next: u64,
    entries: VecDeque<Waiter>,
}
struct Waiter {
    id: u64,
    target: Address,
    port: u16,
    send: oneshot::Sender<ShadowsocksUdpResponse>,
}
impl Default for Pending {
    fn default() -> Self {
        Self {
            next: 0,
            entries: VecDeque::new(),
        }
    }
}
/// One real pending response registration. Drop removes this exact registration,
/// including before a send completes; it never cancels the shared physical IO.
pub struct ShadowsocksUdpPendingResponse {
    owner: Arc<Responses>,
    id: u64,
    receive: oneshot::Receiver<ShadowsocksUdpResponse>,
    capacity: Option<OwnedSemaphorePermit>,
    total: Option<budget::Charge>,
    pub(in crate::transport::udp_socket) encoded_bytes: usize,
}
impl Responses {
    pub(in crate::transport::udp_socket) fn register(
        self: &Arc<Self>,
        target: &Address,
        port: u16,
    ) -> std::io::Result<ShadowsocksUdpPendingResponse> {
        let capacity = self
            .capacity
            .clone()
            .try_acquire_owned()
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::WouldBlock))?;
        let total = self.budget.claim()?;
        let (send, receive) = oneshot::channel();
        let mut pending = self.pending.lock().unwrap();
        if self.closed.load(std::sync::atomic::Ordering::Acquire) {
            return Err(std::io::ErrorKind::BrokenPipe.into());
        }
        let id = pending.next.checked_add(1).ok_or_else(|| {
            std::io::Error::other("SS UDP response registration identity exhausted")
        })?;
        pending.next = id;
        pending.entries.push_back(Waiter {
            id,
            target: target.clone(),
            port,
            send,
        });
        Ok(ShadowsocksUdpPendingResponse {
            owner: self.clone(),
            id,
            receive,
            capacity: Some(capacity),
            total: Some(total),
            encoded_bytes: 0,
        })
    }
    pub(super) fn deliver_pending(
        &self,
        response: ShadowsocksUdpResponse,
    ) -> Result<(), ShadowsocksUdpResponse> {
        let waiter = {
            let mut pending = self.pending.lock().unwrap();
            let Some(index) = pending
                .entries
                .iter()
                .position(|waiter| waiter.target == response.0 && waiter.port == response.1)
            else {
                return Err(response);
            };
            pending.entries.remove(index).unwrap()
        };
        // Original semantics: the first actual registered target waiter consumes
        // this response, even if its receiver is concurrently being cancelled.
        let _ = waiter.send.send(response);
        Ok(())
    }
    pub(in crate::transport::udp_socket) fn close_pending(&self) {
        let returned = {
            let mut pending = self.pending.lock().unwrap();
            self.closed
                .store(true, std::sync::atomic::Ordering::Release);
            std::mem::take(&mut pending.entries)
        };
        drop(returned);
    }
    pub(super) fn remove(&self, id: u64) {
        let removed = {
            let mut pending = self.pending.lock().unwrap();
            pending
                .entries
                .iter()
                .position(|waiter| waiter.id == id)
                .and_then(|index| pending.entries.remove(index))
        };
        drop(removed);
    }
}
impl Future for ShadowsocksUdpPendingResponse {
    type Output = std::io::Result<ShadowsocksUdpResponse>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.receive).poll(cx).map(|outcome| {
            outcome.map_err(|source| std::io::Error::new(std::io::ErrorKind::BrokenPipe, source))
        });
        if result.is_ready() {
            drop(this.capacity.take());
            drop(this.total.take());
        }
        result
    }
}
impl Drop for ShadowsocksUdpPendingResponse {
    fn drop(&mut self) {
        self.owner.remove(self.id);
    }
}
pub(super) fn capacity(limit: usize) -> Arc<Semaphore> {
    Arc::new(Semaphore::new(limit))
}

impl ShadowsocksUdpPendingResponse {
    #[cfg(test)]
    pub(in crate::transport::udp_socket) fn received_for_test(&self) -> bool {
        !self.receive.is_empty()
    }
    /// Actual locally accepted protocol-frame length, not peer delivery.
    pub fn encoded_bytes(&self) -> usize {
        self.encoded_bytes
    }
}

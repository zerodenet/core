//! One serialized, cancellation-safe receive owner for a Direct socket set.
use super::{DirectUdpResponseSource, DirectUdpSocket, DirectUdpSockets};
use std::{
    future::poll_fn,
    io,
    net::SocketAddr,
    task::{Context, Poll},
};

const MAX_DATAGRAM: usize = 65_535;

#[derive(Default)]
pub(super) struct DirectUdpReceiver {
    pub(super) cursor: usize,
    // Short caller buffers must still consume the complete datagram, preserving
    // native error/observation semantics. Normal full-size callers need none.
    pub(super) scratch: Vec<u8>,
}

impl DirectUdpSockets {
    pub(super) fn reset_receiver_if_empty(&mut self) {
        if self.sockets.is_empty() {
            *self.receive.get_mut() = DirectUdpReceiver::default();
        }
    }

    pub(crate) async fn recv_from_addr(
        &self,
        output: &mut [u8],
    ) -> io::Result<(usize, DirectUdpResponseSource)> {
        // Tokio retains one receive waker per native socket. Serialize readers
        // across the set; cancelling a waiter/owner releases the guard. No
        // read-ahead task can consume replies after cancellation.
        let mut receiver = self.receive.lock().await;
        if self.sockets.is_empty() {
            return std::future::pending().await;
        }
        let needs_scratch = output.len() < MAX_DATAGRAM;
        if needs_scratch {
            receiver.scratch.resize(MAX_DATAGRAM, 0);
        }
        loop {
            let start = receiver.cursor % self.sockets.len();
            let bytes = if needs_scratch {
                receiver.scratch.as_mut_slice()
            } else {
                &mut *output
            };
            let (index, result) = poll_fn(|cx| self.poll_receive(cx, start, bytes)).await;
            // Round robin survives cancelled receives and also advances for
            // errors/untrusted peers, so a noisy socket cannot monopolize I/O.
            receiver.cursor = (index + 1) % self.sockets.len();
            let (size, sender) = result?;
            let entry = &self.sockets[index];
            if self.receive_is_current(entry) {
                if let Ok(sender) = entry.binding.policy.dial_policy.normalize_peer(sender) {
                    if let Some(flow) = entry.replies.flow(&sender) {
                        let size = size.min(output.len());
                        if needs_scratch {
                            output[..size].copy_from_slice(&receiver.scratch[..size]);
                        }
                        return Ok((
                            size,
                            DirectUdpResponseSource {
                                sender,
                                session_id: Some(flow.session_id),
                                guard: Some(flow.guard.clone()),
                            },
                        ));
                    }
                }
            }
            // Bound work when a socket continuously receives rejected replies.
            tokio::task::yield_now().await;
        }
    }

    fn receive_is_current(&self, entry: &DirectUdpSocket) -> bool {
        !entry.retired
            && self.generation == self.services.egress_generation()
            && self
                .services
                .direct_policy_is_current(&entry.binding.policy)
    }

    fn poll_receive(
        &self,
        cx: &mut Context<'_>,
        start: usize,
        bytes: &mut [u8],
    ) -> Poll<(usize, io::Result<(usize, SocketAddr)>)> {
        for offset in 0..self.sockets.len() {
            let index = (start + offset) % self.sockets.len();
            let entry = &self.sockets[index];
            if self.receive_is_current(entry) {
                if let Poll::Ready(result) = entry.socket.poll_recv_from_addr(cx, bytes) {
                    return Poll::Ready((index, result));
                }
            }
        }
        // Empty/stale sets stay parked; admission/reload callers cancel this
        // receive in their existing select loop before mutating the socket set.
        Poll::Pending
    }
}

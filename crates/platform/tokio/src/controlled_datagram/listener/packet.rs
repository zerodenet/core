//! Ancillary sending on the same original descriptor and bounded writer gate.
use super::*;
use crate::{packet_socket::ancillary, EgressInterfaceControl, PacketScopeRestoreFailure};
use std::task::{Context, Poll};
use tokio::io::Interest;

impl ControlledDatagramListenerTx {
    pub(crate) async fn send_packet(
        &self,
        bytes: &[u8],
        peer: SocketAddr,
        control: &EgressInterfaceControl,
    ) -> std_io::Result<usize> {
        let shared = &self.shared;
        shared.check(*shared.lock())?;
        let _admission = shared.sends.try_acquire().map_err(|_| {
            std_io::Error::new(
                std_io::ErrorKind::WouldBlock,
                "UDP listener send capacity exhausted",
            )
        })?;
        tokio::select! {
            biased;
            _ = shared.socket.entry.retired.cancelled() => Err(shared.cancelled()),
            result = async {
                let _writer = shared.writer.lock().await;
                poll_fn(|cx| shared.poll_packet(cx, bytes, peer, control)).await
            } => result,
        }
    }
}
impl Shared {
    fn poll_packet(
        &self,
        cx: &mut Context<'_>,
        bytes: &[u8],
        peer: SocketAddr,
        control: &EgressInterfaceControl,
    ) -> Poll<std_io::Result<usize>> {
        let mut prepared = match self
            .socket
            .prepare_poll(super::super::wake_relay::Direction::Write, cx)
        {
            Ok(prepared) => prepared,
            Err(error) => return Poll::Ready(Err(error)),
        };
        let retired = self.lock();
        if let Err(error) = self.check(*retired) {
            return Poll::Ready(Err(error));
        }
        let native = self.socket.entry.lock();
        let NativeHandleState::Live(socket) = &*native else {
            return Poll::Ready(Err(super::super::io::unavailable(&native)));
        };
        let observer = socket.observer.clone();
        let (result, cancel) =
            match poll_packet(socket, &mut prepared.context(), bytes, peer, control) {
                Poll::Ready(Err(error))
                    if error
                        .get_ref()
                        .is_some_and(|cause| cause.is::<PacketScopeRestoreFailure>()) =>
                {
                    // Persist before dropping gates or invoking external code.
                    let source = Arc::new(error);
                    *self
                        .socket
                        .entry
                        .packet_failure
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(source.clone());
                    (Poll::Ready(Err(std_io::Error::other(source))), true)
                }
                result => (result, false),
            };
        drop(native);
        drop(retired);
        if cancel {
            self.socket.entry.retire_io();
        }
        if let Poll::Ready(result) = &result {
            observe(observer, result);
        }
        prepared.pending(result.is_pending());
        result
    }
}

fn poll_packet(
    socket: &TokioDatagramSocket,
    cx: &mut Context<'_>,
    bytes: &[u8],
    peer: SocketAddr,
    control: &EgressInterfaceControl,
) -> Poll<std_io::Result<usize>> {
    let result = loop {
        match socket.inner.poll_send_ready(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(error)) => break Err(error),
            Poll::Ready(Ok(())) => {}
        }
        // Select at the actual syscall boundary, including after readiness
        // suspension. There is no retained stale route hint or unmarked retry.
        let selection = match control.select_packet_without_native_probe(peer) {
            Ok(selection) => selection,
            Err(error) => break Err(error),
        };
        if let Err(error) = selection.ensure_connectable() {
            break Err(error);
        }
        #[cfg(not(target_os = "linux"))]
        if selection
            .interface()
            .is_some_and(|interface| interface.socket_mark().is_some())
        {
            // A retained firewall identity is not proof of a platform leak
            // guard. This new backend has no such grant/receipt to verify.
            break Err(std_io::Error::new(
                std_io::ErrorKind::Unsupported,
                "controlled packet sender has no platform leak-guard receipt for socket mark",
            ));
        }
        if selection.generation() != control.generation() {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        match socket.inner.try_io(Interest::WRITABLE, || {
            ancillary::send(&socket.inner, bytes, peer, selection.interface())
        }) {
            Err(error) if error.kind() == std_io::ErrorKind::WouldBlock => continue,
            result => break result,
        }
    };
    Poll::Ready(result)
}
fn observe(observer: super::super::observation::Observer, result: &std_io::Result<usize>) {
    match result {
        Ok(size) => {
            if let Some(observer) = observer {
                observer.sent_datagram(*size);
            }
        }
        Err(error) => observe_error(observer, error),
    }
}
fn observe_error(observer: super::super::observation::Observer, error: &std_io::Error) {
    if let Some(observer) = observer {
        // Restoration can fail after an actual successful send. Count
        // submission once, independently of its persistent policy fault.
        if let Some(failure) = PacketScopeRestoreFailure::from_io(error) {
            if let Some(size) = failure.submitted_bytes() {
                observer.sent_datagram(size);
            }
        }
        observer.error();
    }
}

#[cfg(test)]
mod tests;

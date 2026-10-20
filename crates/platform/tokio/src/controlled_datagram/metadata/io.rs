use super::*;
use std::{io::IoSliceMut, task::Context};
use tokio::io::Interest;
impl MetadataDatagramTx {
    pub fn try_send(&self, transmit: &MetadataTransmit<'_>) -> std_io::Result<MetadataSendOutcome> {
        self.shared.check()?;
        // This bounds callers inside or waiting for the native gate, not the
        // number of long-lived TX views. There is no outside waiting queue.
        let _admission = self.shared.sends.try_acquire().map_err(|_| {
            std_io::Error::new(
                std_io::ErrorKind::WouldBlock,
                "UDP native send call capacity exhausted",
            )
        })?;
        validate(transmit, self.shared.capabilities())?;
        let _defer = self.shared.pollers.defer();
        let native = self.shared.socket.entry.lock();
        self.shared.check()?;
        let NativeHandleState::Live(socket) = &*native else {
            return Err(super::super::io::unavailable(&native));
        };
        let native_transmit = quinn_udp::Transmit {
            destination: transmit.destination,
            contents: transmit.contents,
            src_ip: transmit.source_ip,
            ecn: transmit.ecn.map(DatagramEcn::native),
            segment_size: transmit.segment_size,
        };
        let outcome = socket.inner.try_io(Interest::WRITABLE, || {
            self.shared
                .state
                .try_send_outcome((&socket.inner).into(), &native_transmit)
        })?;
        Ok(MetadataSendOutcome {
            bytes: outcome.bytes,
            datagrams: outcome.datagrams,
            complete: outcome.complete,
        })
    }
}
fn validate(transmit: &MetadataTransmit<'_>, caps: MetadataCapabilities) -> std_io::Result<()> {
    if let Some(size) = transmit.segment_size {
        if size == 0 || size > u16::MAX as usize {
            return Err(std_io::Error::new(
                std_io::ErrorKind::InvalidInput,
                "invalid native UDP segment size",
            ));
        }
        if transmit.contents.len().div_ceil(size).max(1) > caps.transmit_segments {
            return Err(std_io::Error::new(
                std_io::ErrorKind::Unsupported,
                "UDP segment count exceeds actual native capability",
            ));
        }
    }
    Ok(())
}
impl MetadataDatagramRx {
    pub fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
        buffers: &mut [IoSliceMut<'_>],
        metadata: &mut [MetadataReceive],
    ) -> Poll<std_io::Result<usize>> {
        if self
            .registration
            .prepare(cx, &self.shared.socket.entry.retired)
        {
            return Poll::Ready(self.shared.check().map(|()| 0));
        }
        if buffers.is_empty() || metadata.len() < buffers.len().min(quinn_udp::BATCH_SIZE) {
            return Poll::Ready(Err(std_io::Error::new(
                std_io::ErrorKind::InvalidInput,
                "UDP receive metadata capacity is too small",
            )));
        }
        let waker = self.registration.waker();
        let mut internal = Context::from_waker(&waker);
        let _defer = self.shared.pollers.defer();
        let native = self.shared.socket.entry.lock();
        self.shared.check()?;
        let NativeHandleState::Live(socket) = &*native else {
            return Poll::Ready(Err(super::super::io::unavailable(&native)));
        };
        loop {
            match socket.inner.poll_recv_ready(&mut internal) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(())) => {}
            }
            let mut native_metadata = [quinn_udp::RecvMeta::default(); quinn_udp::BATCH_SIZE];
            let count = buffers.len().min(quinn_udp::BATCH_SIZE);
            match socket.inner.try_io(Interest::READABLE, || {
                self.shared.state.recv(
                    (&socket.inner).into(),
                    &mut buffers[..count],
                    &mut native_metadata[..count],
                )
            }) {
                Err(error) if error.kind() == std_io::ErrorKind::WouldBlock => continue,
                Err(error) => return Poll::Ready(Err(error)),
                Ok(count) => {
                    for (out, native) in metadata.iter_mut().zip(native_metadata).take(count) {
                        *out = MetadataReceive::from_native(native);
                    }
                    return Poll::Ready(Ok(count));
                }
            }
        }
    }
}

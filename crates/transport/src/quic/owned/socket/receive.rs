use super::*;
use std::task::{Context, Poll};
use zero_platform_tokio::controlled_datagram::metadata::{DatagramEcn, MetadataReceive};

struct ReceiveLease<'a> {
    slot: &'a Mutex<Option<MetadataDatagramRx>>,
    receiver: Option<MetadataDatagramRx>,
}
impl Drop for ReceiveLease<'_> {
    fn drop(&mut self) {
        // Only this lease may restore the unique receiver. No external Waker or
        // observer is polled/dropped while the transport mutex is held.
        *self.slot.lock().unwrap() = self.receiver.take();
    }
}
impl MetadataSocket {
    pub(super) fn receive(
        &self,
        cx: &mut Context<'_>,
        buffers: &mut [io::IoSliceMut<'_>],
        metadata: &mut [quinn::udp::RecvMeta],
    ) -> Poll<io::Result<usize>> {
        self.callbacks.check()?;
        let count = buffers.len().min(quinn::udp::BATCH_SIZE);
        if count == 0 || metadata.len() < count {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Quinn metadata receive capacity is too small",
            )));
        }
        let receiver = self.receiver.lock().unwrap().take();
        let Some(receiver) = receiver else {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Quinn endpoint attempted concurrent native reception",
            )));
        };
        let mut lease = ReceiveLease {
            slot: &self.receiver,
            receiver: Some(receiver),
        };
        let mut neutral = [MetadataReceive::default(); quinn::udp::BATCH_SIZE];
        for _ in 0..32 {
            match lease.receiver.as_mut().unwrap().poll_recv(
                cx,
                &mut buffers[..count],
                &mut neutral[..count],
            ) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) if is_transient_native_recv_error(&error) => {
                    let source = Arc::new(error);
                    {
                        let mut drops = self.drops.lock().unwrap();
                        match drops.count.checked_add(1) {
                            Some(count) => drops.count = count,
                            None => drops.overflowed = true,
                        }
                        if drops.first.is_none() {
                            drops.first = Some(source.clone());
                        }
                    }
                    tracing::debug!(error = %source, "owned Quinn native UDP receive transient error dropped");
                }
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(count)) => {
                    for (target, source) in metadata.iter_mut().zip(neutral).take(count) {
                        *target = quinn::udp::RecvMeta {
                            addr: source.source,
                            dst_ip: source.destination_ip,
                            len: source.len,
                            stride: source.stride,
                            ecn: source.ecn.map(|ecn| match ecn {
                                DatagramEcn::Ect0 => quinn::udp::EcnCodepoint::Ect0,
                                DatagramEcn::Ect1 => quinn::udp::EcnCodepoint::Ect1,
                                DatagramEcn::Ce => quinn::udp::EcnCodepoint::Ce,
                            }),
                        };
                    }
                    return Poll::Ready(Ok(count));
                }
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

fn is_transient_native_recv_error(error: &io::Error) -> bool {
    if matches!(
        error.kind(),
        io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionRefused
            | io::ErrorKind::NetworkDown
            | io::ErrorKind::NetworkUnreachable
            | io::ErrorKind::HostUnreachable
            | io::ErrorKind::AddrNotAvailable
    ) {
        return true;
    }
    #[cfg(unix)]
    if matches!(
        error.raw_os_error(),
        Some(
            libc::ENETDOWN
                | libc::ENETUNREACH
                | libc::EHOSTUNREACH
                | libc::EADDRNOTAVAIL
                | libc::ECONNREFUSED
                | libc::ECONNRESET
                | libc::ENOBUFS
        )
    ) {
        return true;
    }
    false
}

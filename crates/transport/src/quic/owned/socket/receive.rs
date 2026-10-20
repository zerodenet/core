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
        match lease.receiver.as_mut().unwrap().poll_recv(
            cx,
            &mut buffers[..count],
            &mut neutral[..count],
        ) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
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
                Poll::Ready(Ok(count))
            }
        }
    }
}

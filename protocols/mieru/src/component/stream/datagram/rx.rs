use super::{shared::Shared, *};
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::ReadBuf;
use zero_core::{component::AssociationId, packet::Datagram, request::Destination};
use zero_platform_tokio::contract::{datagram::DatagramRx, BoxFuture};

pub struct Rx {
    shared: Arc<Shared>,
    association: AssociationId,
    limits: DatagramLimits,
    pending: Vec<u8>,
    cancel: BoxFuture<'static, ()>,
}
impl Rx {
    pub(super) fn new(
        shared: Arc<Shared>,
        association: AssociationId,
        limits: DatagramLimits,
    ) -> Self {
        Self {
            cancel: shared.cancellation(),
            shared,
            association,
            limits,
            pending: Vec::new(),
        }
    }
}
impl DatagramRx for Rx {
    fn poll_recv(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<Datagram>, ChannelError>> {
        let this = self.get_mut();
        this.shared.check(&mut this.cancel, cx, Stage::Receive)?;
        loop {
            let needed = if this.pending.len() < 3 {
                3
            } else {
                if this.pending[0] != 0 {
                    this.shared.stop.cancel();
                    return Poll::Ready(Err(this.shared.error(
                        Stage::Receive,
                        Reason::Protocol,
                        DeliveryState::Unknown,
                    )));
                }
                4 + usize::from(u16::from_be_bytes([this.pending[1], this.pending[2]]))
            };
            if this.pending.len() == needed && needed >= 4 {
                let packet = crate::udp::MieruUdpFlowCodec
                    .decode_packet(&std::mem::take(&mut this.pending))
                    .map_err(|source| {
                        this.shared.stop.cancel();
                        this.shared
                            .error(Stage::Receive, Reason::Protocol, DeliveryState::Unknown)
                            .with_source(std::io::Error::other(source))
                    })?;
                let (address, port, payload) = packet.into_parts();
                let frame = Datagram {
                    association: this.association,
                    address: Destination::new(address, port),
                    payload: payload.into(),
                };
                // Fixed targets constrain sends. The authenticated proxy may
                // report a resolved source IP for a domain destination.
                if frame.payload.len() > this.limits.max_payload {
                    let mut error = this.shared.error(
                        Stage::Receive,
                        Reason::Capacity,
                        DeliveryState::NotSubmitted,
                    );
                    error.impact = zero_platform_tokio::contract::Impact::Datagram;
                    return Poll::Ready(Err(error));
                }
                return Poll::Ready(Ok(Some(frame)));
            }
            let mut bytes = [0; 4096];
            let limit = bytes.len().min(needed - this.pending.len());
            let mut read = ReadBuf::new(&mut bytes[..limit]);
            let mut state = this.shared.state.lock().unwrap();
            let result = state
                .stream
                .as_mut()
                .expect("live logical stream")
                .as_mut()
                .poll_read(cx, &mut read);
            drop(state);
            std::task::ready!(result).map_err(|source| {
                this.shared.stop.cancel();
                this.shared.io_error(Stage::Receive, source)
            })?;
            if read.filled().is_empty() {
                return Poll::Ready(if this.pending.is_empty() {
                    Ok(None)
                } else {
                    this.shared.stop.cancel();
                    Err(this
                        .shared
                        .error(Stage::Receive, Reason::Protocol, DeliveryState::Unknown))
                });
            }
            this.pending.extend_from_slice(read.filled());
        }
    }
}
impl Drop for Rx {
    fn drop(&mut self) {
        self.shared.return_view();
    }
}

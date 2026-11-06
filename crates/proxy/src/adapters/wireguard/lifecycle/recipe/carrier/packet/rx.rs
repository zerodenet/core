use super::*;
use std::task::{Context, Poll};
use zero_core::packet::IpPacket;
use zero_platform_tokio::contract::packet::PacketRx;
pub(super) struct Rx {
    pub(super) shared: Arc<Shared>,
    pub(super) cancelled: Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
}
impl PacketRx for Rx {
    fn poll_recv(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<IpPacket>, ChannelError>> {
        if self.cancelled.as_mut().poll(cx).is_ready() {
            self.shared.stop();
        }
        let shared = &self.shared;
        if shared.cancel.is_cancelled() {
            return Poll::Ready(Ok(None));
        }
        let mut state = shared.state.lock().unwrap();
        state.receiver = Some(cx.waker().clone());
        let count = state.peers.len();
        for offset in 0..count {
            let index = (state.next + offset) % count;
            let peer = &mut state.peers[index];
            match peer.rx.poll_recv(cx) {
                Poll::Ready(Some(frame)) => {
                    let actual_peer = peer.id;
                    state.next = (index + 1) % count;
                    let Some(association) = shared.returned(&state, actual_peer, &frame) else {
                        let associations = shared.coverage_lost(&mut state);
                        drop(state);
                        if let Some(observer) = &shared.observer {
                            for association in associations {
                                observer.packet_io(&association, zero_platform_tokio::contract::context::PacketIoFact::ReceiveCoverageLost);
                            }
                        }
                        // The bounded queue may contain further real frames;
                        // retain readiness even when this unmatched one drops.
                        cx.waker().wake_by_ref();
                        return Poll::Pending;
                    };
                    let packet = IpPacket::new(frame, association).map_err(|source| {
                        error(Stage::Receive, Reason::Protocol).with_source(std::io::Error::other(
                            format!("native IP envelope: {source:?}"),
                        ))
                    })?;
                    return Poll::Ready(Ok(Some(packet)));
                }
                Poll::Ready(None) | Poll::Pending => {}
            }
        }
        Poll::Pending
    }
}

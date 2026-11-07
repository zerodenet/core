//! Actual translated responses retain their native allocation through the
//! same pending return admission; cache images never escape as bare Vecs.
use super::*;
use zero_platform_tokio::contract::{ChannelError, Impact, Reason, Stage};
use zero_stack::echo_translation::OwnedEchoResponse;
impl PacketReturns {
    pub(crate) async fn deliver_correlated_owned_wait(
        &self,
        mut original: zero_traits::PacketBuffer,
    ) -> Result<Option<zero_traits::PacketBuffer>, ChannelError> {
        loop {
            let changed = self.budget.changed();
            tokio::pin!(changed);
            let mut signalled = false;
            // Register before observing the original cache/capacity. The same
            // producing wire keeps TX/timers progressing while this parks RX.
            std::future::poll_fn(|cx| {
                use std::future::Future;
                signalled = changed.as_mut().poll(cx).is_ready();
                std::task::Poll::Ready(())
            })
            .await;
            let metadata = self
                .translated
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .response_context(&original, Instant::now())
                .cloned();
            let Some(metadata) = metadata else {
                return Ok(Some(original));
            };
            let mtu = metadata
                .replies
                .return_mtu()
                .ok_or_else(|| invalid("translated return Source omitted link MTU"))?;
            let wire = metadata
                .replies
                .wire_memory()
                .ok_or_else(|| invalid("translated return Source omitted memory layout"))?;
            let attempt = {
                let _notifications = self.budget.defer_notifications();
                let mut owner = self
                    .translated
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                let outcome =
                    owner.response_owned(original, usize::from(mtu), wire, Instant::now());
                drop(owner);
                outcome
            };
            match attempt {
                Ok(OwnedEchoResponse::Unmatched(packet)) => return Ok(Some(packet)),
                Ok(OwnedEchoResponse::Response(
                    TranslatedReturn { replies, observer },
                    mut restored,
                )) => {
                    if let Some(observer) = &observer {
                        observer.received(restored.len());
                    }
                    if !packet::advance_ip_hop(&mut restored) {
                        self.discard(observer.as_deref(), zero_traits::PacketDropReason::HopLimit);
                        return Ok(None);
                    }
                    let storage = self.retain_transmit_reply_wait(restored, mtu, wire).await?;
                    if replies.send_buffer(storage).await.is_err() {
                        self.discard(
                            observer.as_deref(),
                            zero_traits::PacketDropReason::QueueClosed,
                        );
                    }
                    return Ok(None);
                }
                Err(failure) if failure.source.kind() == io::ErrorKind::WouldBlock => {
                    original = failure.original;
                    if !signalled {
                        changed.await;
                    }
                }
                Err(failure) => {
                    return Err(
                        ChannelError::new(Stage::Receive, Reason::Capacity, Impact::Packet)
                            .with_source(failure),
                    )
                }
            }
        }
    }
}
fn invalid(message: &'static str) -> ChannelError {
    ChannelError::new(Stage::Receive, Reason::InvalidConfig, Impact::Component)
        .with_source(io::Error::other(message))
}

impl PacketReturns {
    pub(crate) fn deliver_correlated(&self, packet: &[u8]) -> bool {
        if let Some((TranslatedReturn { replies, observer }, mut restored)) = self
            .translated
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .response(packet, Instant::now())
        {
            if let Some(observer) = &observer {
                observer.received(packet.len());
            }
            if packet::advance_ip_hop(&mut restored) {
                self.send_reply(&replies, restored.into(), observer.as_deref());
            } else {
                self.discard(observer.as_deref(), zero_traits::PacketDropReason::HopLimit);
            }
            return true;
        }
        false
    }

    pub(crate) fn translate(
        &self,
        packet: &[u8],
        local: IpAddr,
        replies: zero_stack::packet_output::PacketSender,
        observer: Option<Arc<dyn zero_traits::IoObserver>>,
    ) -> io::Result<Vec<u8>> {
        self.translated
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .request(
                packet,
                local,
                TranslatedReturn { replies, observer },
                Instant::now(),
            )
    }

    pub(crate) fn translate_owned(
        &self,
        packet: zero_traits::PacketBuffer,
        local: IpAddr,
        replies: zero_stack::packet_output::PacketSender,
        observer: Option<Arc<dyn zero_traits::IoObserver>>,
        mtu: usize,
        wire: zero_stack::packet_output::memory::PacketWireMemory,
    ) -> Result<Vec<zero_traits::PacketBuffer>, zero_stack::echo_translation::OwnedTranslationFailure>
    {
        let Some((return_mtu, return_wire)) = replies.return_mtu().zip(replies.wire_memory())
        else {
            return Err(zero_stack::echo_translation::OwnedTranslationFailure {
                source: io::Error::other("actual translated Source omitted return memory metadata"),
                original: packet,
            });
        };
        // Accounting returns immediately; external callbacks run only after
        // the actual correlation owner lock has been released on every path.
        let _notifications = self.budget.defer_notifications();
        self.translated
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .request_owned(
                packet,
                local,
                TranslatedReturn { replies, observer },
                mtu,
                wire,
                usize::from(return_mtu),
                return_wire,
                Instant::now(),
            )
    }
}

//! Bounded reverse delivery for packets forwarded through a shared device.

use std::{
    collections::HashMap,
    io,
    net::IpAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

mod budget;
pub(crate) use budget::PacketReturnTotals;
mod delivery;
mod maintenance;
mod registration;

use tokio::sync::mpsc;
use zero_stack::packet;

const MAX_RETURN_ROUTES: usize = 1_024;
const MAX_OBSERVATIONS: usize = 4_096;
const IDLE_TIMEOUT: Duration = Duration::from_secs(300);

#[derive(Default)]
pub(crate) struct PacketReturns {
    budget: budget::ReturnBudget,
    receive_incomplete: std::sync::atomic::AtomicBool,
    routes: Mutex<HashMap<IpAddr, ReturnRoute>>,
    drop_observer: Option<Arc<dyn zero_traits::IoObserver>>,
    observations: Mutex<HashMap<packet::PacketConversationKey, ReturnObservation>>,
    conversations: Mutex<HashMap<packet::PacketConversationKey, ReturnConversation>>,
    translated: Mutex<zero_stack::echo_translation::EchoTranslation<TranslatedReturn>>,
}

struct ReturnConversation {
    replies: zero_stack::packet_output::PacketSender,
    touched: Instant,
}
struct ReturnObservation {
    observer: Option<Arc<dyn zero_traits::IoObserver>>,
    touched: Instant,
}
struct TranslatedReturn {
    replies: zero_stack::packet_output::PacketSender,
    observer: Option<Arc<dyn zero_traits::IoObserver>>,
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) enum ReturnSource {
    Legacy(u64),
    Endpoint {
        endpoint: zero_core::component::EndpointId,
        generation: zero_core::component::ResourceGeneration,
        peer: Option<zero_core::component::PeerId>,
    },
}
struct ReturnRoute {
    ingress_id: ReturnSource,
    replies: zero_stack::packet_output::PacketSender,
    touched: Instant,
}

// Borrowed host reads allocate only after a native return route is found.
enum NativePacket<'a> {
    Borrowed(&'a [u8]),
    Owned(zero_traits::PacketBuffer),
}
impl AsRef<[u8]> for NativePacket<'_> {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Borrowed(bytes) => bytes,
            Self::Owned(bytes) => bytes,
        }
    }
}
impl NativePacket<'_> {
    fn into_owned(self) -> zero_traits::PacketBuffer {
        match self {
            Self::Borrowed(bytes) => bytes.to_vec().into(),
            Self::Owned(bytes) => bytes,
        }
    }
}

impl PacketReturns {
    /// Preserve forwarding when the correlated boundary cannot account for
    /// every received fragment. Never publish a partial count as complete.
    pub(crate) fn lose_receive_coverage(&self) {
        if !self
            .receive_incomplete
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            for observation in self
                .observations
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .values()
            {
                if let Some(observer) = &observation.observer {
                    observer.receive_coverage_lost();
                }
            }
        }
    }
    pub(crate) fn with_drop_observer(
        drop_observer: Option<Arc<dyn zero_traits::IoObserver>>,
    ) -> Self {
        Self {
            drop_observer,
            ..Self::default()
        }
    }
    pub(crate) fn with_limits(
        mut self,
        options: &super::super::RawIpStackOptions,
    ) -> std::io::Result<Self> {
        options.packet_return_totals.install(
            options.packet_return_total_max_frames,
            options.packet_return_total_max_bytes,
        )?;
        self.budget = budget::ReturnBudget::with_totals(
            options.packet_return_max_frames,
            options.packet_return_max_bytes,
            options.packet_return_totals.clone(),
        );
        Ok(self)
    }
    pub(crate) fn retain_reply_buffer_for(
        &self,
        packet: zero_traits::PacketBuffer,
        return_mtu: u16,
    ) -> Result<zero_traits::PacketBuffer, zero_traits::PacketBuffer> {
        self.budget
            .retain_for_return(packet, return_mtu)
            .map_err(|failure| {
                tracing::trace!(cause = ?failure.cause, bytes = failure.original.len(), "same original Packet return admission rejected");
                failure.original
            })
    }
    pub(super) fn queue_reply(
        &self,
        replies: &zero_stack::packet_output::PacketSender,
        packet: zero_traits::PacketBuffer,
    ) -> Result<(), mpsc::error::TrySendError<zero_traits::PacketBuffer>> {
        let packet = match replies.return_mtu() {
            Some(mtu) => self
                .budget
                .retain_for_return(packet, mtu)
                .map_err(|failure| {
                tracing::trace!(cause = ?failure.cause, bytes = failure.original.len(), "same original Packet return admission rejected");
                failure.original
            }),
            // Explicit pre-standard library sinks only. Actual Source sinks
            // carry their separately validated original link MTU.
            None => self.budget.retain(packet),
        }
        .map_err(mpsc::error::TrySendError::Full)?;
        replies.try_send_buffer(packet)
    }
    fn discard(
        &self,
        observer: Option<&dyn zero_traits::IoObserver>,
        reason: zero_traits::PacketDropReason,
    ) {
        if let Some(observer) = observer {
            observer.dropped_reason(reason);
        }
        if let Some(observer) = &self.drop_observer {
            observer.dropped_reason(reason);
        }
    }
    fn send_reply(
        &self,
        replies: &zero_stack::packet_output::PacketSender,
        packet: zero_traits::PacketBuffer,
        observer: Option<&dyn zero_traits::IoObserver>,
    ) {
        if let Err(error) = self.queue_reply(replies, packet) {
            self.discard(
                observer,
                match error {
                    mpsc::error::TrySendError::Full(_) => zero_traits::PacketDropReason::QueueFull,
                    mpsc::error::TrySendError::Closed(_) => {
                        zero_traits::PacketDropReason::QueueClosed
                    }
                },
            );
        }
    }

    pub(crate) fn deliver(&self, packet: &[u8]) -> bool {
        if self.deliver_correlated(packet) {
            return true;
        }
        self.deliver_native(NativePacket::Borrowed(packet)).is_ok()
    }

    /// Move a native return into its bounded channel; misses return ownership.
    pub(crate) fn deliver_owned(
        &self,
        packet: zero_traits::PacketBuffer,
    ) -> Result<(), zero_traits::PacketBuffer> {
        if self.deliver_correlated(&packet) {
            return Ok(());
        }
        self.deliver_native(NativePacket::Owned(packet))
            .map_err(NativePacket::into_owned)
    }

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
}

#[cfg(test)]
#[path = "returns/tests.rs"]
mod tests;

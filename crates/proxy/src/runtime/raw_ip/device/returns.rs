//! Bounded reverse delivery for packets forwarded through a shared device.

use std::{
    collections::HashMap,
    io,
    net::IpAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use tokio::sync::mpsc;
use zero_stack::packet;

const MAX_RETURN_ROUTES: usize = 1_024;
const MAX_OBSERVATIONS: usize = 4_096;
const IDLE_TIMEOUT: Duration = Duration::from_secs(300);

#[derive(Default)]
pub(crate) struct PacketReturns {
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

struct ReturnRoute {
    ingress_id: u64,
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
        if let Err(error) = replies.try_send_buffer(packet) {
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

    #[cfg(test)]
    pub(crate) fn register(
        &self,
        source: IpAddr,
        ingress_id: u64,
        replies: zero_stack::packet_output::PacketSender,
    ) -> io::Result<()> {
        self.register_observed(source, ingress_id, replies, None, None)
    }
    pub(crate) fn register_observed(
        &self,
        source: IpAddr,
        ingress_id: u64,
        replies: zero_stack::packet_output::PacketSender,
        observer: Option<Arc<dyn zero_traits::IoObserver>>,
        conversation: Option<packet::PacketConversationKey>,
    ) -> io::Result<()> {
        let now = Instant::now();
        // Admit both route and observation before publishing either. A rejected
        // overlapping ingress must not replace the accepted flow's observer.
        let mut routes = self.routes.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(route) = routes.get(&source) {
            if route.ingress_id != ingress_id
                && now.duration_since(route.touched) < IDLE_TIMEOUT
                && !route.replies.is_closed()
            {
                return Err(io::Error::new(
                    io::ErrorKind::AddrInUse,
                    "overlapping packet source on shared outbound",
                ));
            }
        } else {
            if routes.len() >= MAX_RETURN_ROUTES {
                routes.retain(|_, route| {
                    now.duration_since(route.touched) < IDLE_TIMEOUT && !route.replies.is_closed()
                });
            }
            if routes.len() >= MAX_RETURN_ROUTES {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "packet return route limit",
                ));
            }
        }
        // Return channel ownership is mandatory execution state, with its own
        // bounded capacity. It is distinct from optional metric observations.
        let mut conversations = self.conversations.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(key) = conversation {
            if !conversations.contains_key(&key) && conversations.len() >= 8192 {
                conversations.retain(|_, c| c.touched.elapsed() < Duration::from_secs(600));
                if conversations.len() >= 8192 {
                    return Err(io::Error::new(
                        io::ErrorKind::WouldBlock,
                        "packet return conversation limit",
                    ));
                }
            }
            conversations.insert(
                key,
                ReturnConversation {
                    replies: replies.clone(),
                    touched: now,
                },
            );
        }
        let mut observations = self.observations.lock().unwrap_or_else(|e| e.into_inner());
        if let (Some(observer), Some(key)) = (observer, conversation) {
            if self
                .receive_incomplete
                .load(std::sync::atomic::Ordering::Acquire)
            {
                observer.receive_coverage_lost();
            }
            if !observations.contains_key(&key) && observations.len() >= MAX_OBSERVATIONS {
                observer.receive_coverage_lost();
            } else {
                observations.insert(
                    key,
                    ReturnObservation {
                        observer: Some(observer),
                        touched: now,
                    },
                );
            }
        }
        routes.insert(
            source,
            ReturnRoute {
                ingress_id,
                replies,
                touched: now,
            },
        );
        Ok(())
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

    fn deliver_native<'a>(&self, packet: NativePacket<'a>) -> Result<(), NativePacket<'a>> {
        let Some(destination) = packet::ip_destination(packet.as_ref()) else {
            return Err(packet);
        };
        let replies = self
            .routes
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&destination)
            .filter(|route| route.touched.elapsed() < IDLE_TIMEOUT)
            .map(|route| route.replies.clone());
        let Some(replies) = replies else {
            return Err(packet);
        };
        let observation = packet::packet_return_key(packet.as_ref()).and_then(|key| {
            self.observations
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&key)
                .filter(|observation| observation.touched.elapsed() < IDLE_TIMEOUT)
                .and_then(|observation| observation.observer.clone())
        });
        let observer = observation;
        let replies = packet::packet_return_key(packet.as_ref())
            .and_then(|key| {
                self.conversations
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .get(&key)
                    .map(|c| c.replies.clone())
            })
            .unwrap_or(replies);
        if replies.is_closed() {
            if let Some(observer) = &observer {
                observer.received(packet.as_ref().len());
            }
            self.discard(
                observer.as_deref(),
                zero_traits::PacketDropReason::QueueClosed,
            );
            return Ok(());
        }
        if let Some(observer) = &observer {
            observer.received(packet.as_ref().len());
        }
        let mut forwarded = packet.into_owned();
        if packet::advance_ip_hop(&mut forwarded) {
            self.send_reply(&replies, forwarded, observer.as_deref());
        } else {
            self.discard(observer.as_deref(), zero_traits::PacketDropReason::HopLimit);
        }
        Ok(())
    }

    pub(crate) fn clear(&self) {
        self.conversations
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
        self.observations
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
        self.translated
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clear();
        self.routes
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clear();
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

    pub(crate) fn expire(&self) {
        self.conversations
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|_, c| c.touched.elapsed() < Duration::from_secs(600));
        self.observations
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|_, observation| observation.touched.elapsed() < IDLE_TIMEOUT);
        self.translated
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .expire(Instant::now(), |route| route.replies.is_closed());
    }
}

#[cfg(test)]
#[path = "returns/tests.rs"]
mod tests;

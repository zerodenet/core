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
pub(super) struct PacketReturns {
    routes: Mutex<HashMap<IpAddr, ReturnRoute>>,
    drop_observer: Option<Arc<dyn zero_traits::IoObserver>>,
    observations: Mutex<HashMap<packet::PacketConversationKey, ReturnObservation>>,
    translated: Mutex<zero_stack::echo_translation::EchoTranslation<TranslatedReturn>>,
}

struct ReturnObservation {
    observer: Arc<dyn zero_traits::IoObserver>,
    touched: Instant,
}
struct TranslatedReturn {
    replies: mpsc::Sender<Vec<u8>>,
    observer: Option<Arc<dyn zero_traits::IoObserver>>,
}

struct ReturnRoute {
    ingress_id: u64,
    replies: mpsc::Sender<Vec<u8>>,
    touched: Instant,
}

impl PacketReturns {
    pub(super) fn with_drop_observer(
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
        replies: &mpsc::Sender<Vec<u8>>,
        packet: Vec<u8>,
        observer: Option<&dyn zero_traits::IoObserver>,
    ) {
        if let Err(error) = replies.try_send(packet) {
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
    pub(super) fn register(
        &self,
        source: IpAddr,
        ingress_id: u64,
        replies: mpsc::Sender<Vec<u8>>,
    ) -> io::Result<()> {
        self.register_observed(source, ingress_id, replies, None, None)
    }
    pub(super) fn register_observed(
        &self,
        source: IpAddr,
        ingress_id: u64,
        replies: mpsc::Sender<Vec<u8>>,
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
        let mut observations = self.observations.lock().unwrap_or_else(|e| e.into_inner());
        if let (Some(observer), Some(key)) = (observer, conversation) {
            if !observations.contains_key(&key) && observations.len() >= MAX_OBSERVATIONS {
                observer.receive_coverage_lost();
            } else {
                observations.insert(
                    key,
                    ReturnObservation {
                        observer,
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

    pub(super) fn deliver(&self, packet: &[u8]) -> bool {
        if self.deliver_correlated(packet) {
            return true;
        }
        self.deliver_native(packet)
    }

    pub(super) fn deliver_correlated(&self, packet: &[u8]) -> bool {
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
                self.send_reply(&replies, restored, observer.as_deref());
            } else {
                self.discard(observer.as_deref(), zero_traits::PacketDropReason::HopLimit);
            }
            return true;
        }
        false
    }

    fn deliver_native(&self, packet: &[u8]) -> bool {
        let Some(destination) = packet::ip_destination(packet) else {
            return false;
        };
        let replies = self
            .routes
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&destination)
            .filter(|route| route.touched.elapsed() < IDLE_TIMEOUT)
            .map(|route| route.replies.clone());
        let Some(replies) = replies else {
            return false;
        };
        let observer = packet::packet_return_key(packet).and_then(|key| {
            self.observations
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&key)
                .filter(|observation| observation.touched.elapsed() < IDLE_TIMEOUT)
                .map(|observation| observation.observer.clone())
        });
        if let Some(observer) = &observer {
            observer.received(packet.len());
        }
        let mut forwarded = packet.to_vec();
        if packet::advance_ip_hop(&mut forwarded) {
            self.send_reply(&replies, forwarded, observer.as_deref());
        } else {
            self.discard(observer.as_deref(), zero_traits::PacketDropReason::HopLimit);
        }
        true
    }

    pub(super) fn clear(&self) {
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

    pub(super) fn translate(
        &self,
        packet: &[u8],
        local: IpAddr,
        replies: mpsc::Sender<Vec<u8>>,
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

    pub(super) fn expire(&self) {
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

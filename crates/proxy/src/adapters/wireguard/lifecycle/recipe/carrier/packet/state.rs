use super::*;
use std::{
    collections::HashMap,
    net::IpAddr,
    time::{Duration, Instant},
};
use tokio::sync::mpsc;
use zero_core::packet::{IpPacket, ReturnAssociation};
use zero_platform_tokio::contract::context::{ConnectObserver, PacketIoFact};
use zero_stack::packet::PacketConversationKey;
pub(super) struct Shared {
    pub(super) carrier: Arc<Carrier>,
    pub(super) caller: zero_platform_tokio::contract::scope::TaskJoinWitness,
    pub(super) users: std::sync::Weak<ViewUsers>,
    pub(super) facts: PacketFacts,
    pub(super) observer: Option<Arc<dyn ConnectObserver>>,
    pub(super) translate: bool,
    pub(super) cancel: CancellationToken,
    pub(super) state: Mutex<State>,
}
#[derive(Default)]
pub(super) struct ViewUsers(Mutex<Vec<crate::runtime::packet_flow::FlowUser>>);
#[derive(Default)]
pub(super) struct State {
    pub(super) peers: Vec<Peer>,
    pub(super) next: usize,
    pub(super) lost: bool,
    pub(super) receiver: Option<std::task::Waker>,
    conversations: HashMap<(usize, PacketConversationKey), Binding>,
    sources: HashMap<(usize, IpAddr), Binding>,
}
pub(super) struct Peer {
    pub(super) id: usize,
    closed: crate::runtime::packet_flow::PacketSourceClose,
    pub(super) tx: mpsc::Sender<zero_traits::PacketBuffer>,
    pub(super) rx: mpsc::Receiver<zero_traits::PacketBuffer>,
}
struct Binding {
    association: ReturnAssociation,
    touched: Instant,
}
impl Shared {
    pub(super) fn stop(&self) {
        self.cancel.cancel();
        let mut state = self.state.lock().unwrap();
        for peer in &mut state.peers {
            peer.rx.close();
            while peer.rx.try_recv().is_ok() {}
        }
        state.conversations.clear();
        state.sources.clear();
        for peer in &state.peers {
            // Confirm only after all actual queues and return mappings closed.
            // This never reports the shared Source main joined or native closed.
            peer.closed.close();
        }
    }
    pub(super) fn sink(
        &self,
        peer: usize,
        frame: &IpPacket,
        device: &crate::runtime::raw_ip::SharedRawIpDevice,
    ) -> Result<(zero_stack::packet_output::PacketSender, ReturnAssociation), ChannelError> {
        let source = zero_stack::packet::ip_source(frame.payload())
            .ok_or_else(|| error(Stage::Send, Reason::Protocol))?;
        let mut state = self.state.lock().unwrap();
        let key = zero_stack::packet::packet_conversation_key(frame.payload());
        let bindings = state.conversations.len() + state.sources.len();
        if bindings >= 8192 {
            state
                .conversations
                .retain(|_, bound| bound.touched.elapsed() < Duration::from_secs(600));
            state
                .sources
                .retain(|_, bound| bound.touched.elapsed() < Duration::from_secs(300));
            if state.conversations.len() + state.sources.len() >= 8192 {
                return Err(error(Stage::Send, Reason::Capacity));
            }
        }
        let source_entry = state
            .sources
            .entry((peer, source))
            .or_insert_with(|| Binding {
                association: frame.return_association.clone(),
                touched: Instant::now(),
            });
        // A same-IP Source authenticated as another peer cannot inherit the
        // already installed native route's reply sink or attribution cookie.
        if source_entry.association.peer != frame.return_association.peer {
            return Err(error(Stage::Send, Reason::InvalidConfig));
        }
        source_entry.touched = Instant::now();
        let fallback = source_entry.association.clone();
        let association = match key {
            Some(key) => {
                let bound = state
                    .conversations
                    .entry((peer, key))
                    .or_insert_with(|| Binding {
                        association: frame.return_association.clone(),
                        touched: Instant::now(),
                    });
                if bound.association.peer != frame.return_association.peer {
                    return Err(error(Stage::Send, Reason::InvalidConfig));
                }
                bound.touched = Instant::now();
                bound.association.clone()
            }
            None => fallback,
        };
        let index = match state.peers.iter().position(|entry| entry.id == peer) {
            Some(index) => index,
            None => {
                let views = self
                    .users
                    .upgrade()
                    .ok_or_else(|| error(Stage::Send, Reason::Revoked))?;
                let (user, closed) =
                    device.open_standard_packet_user(&self.carrier.scope, self.caller.clone())?;
                let (tx, rx) = mpsc::channel(256);
                views.0.lock().unwrap().push(user);
                state.peers.push(Peer {
                    id: peer,
                    tx,
                    rx,
                    closed,
                });
                state.peers.len() - 1
            }
        };
        let sender = state.peers[index].tx.clone().into();
        let receiver = state.receiver.take();
        drop(state);
        if let Some(receiver) = receiver {
            receiver.wake();
        }
        Ok((sender, association))
    }
    pub(super) fn coverage_lost(&self, state: &mut State) -> Vec<ReturnAssociation> {
        if state.lost {
            return Vec::new();
        }
        state.lost = true;
        state
            .sources
            .values()
            .map(|binding| binding.association.clone())
            .collect()
    }
    pub(super) fn returned(
        &self,
        state: &State,
        peer: usize,
        frame: &[u8],
    ) -> Option<ReturnAssociation> {
        if let Some(bound) = zero_stack::packet::packet_return_key(frame)
            .and_then(|key| state.conversations.get(&(peer, key)))
        {
            if bound.touched.elapsed() < Duration::from_secs(600) {
                return Some(bound.association.clone());
            }
        }
        let target = zero_stack::packet::ip_destination(frame)?;
        state
            .sources
            .get(&(peer, target))
            .filter(|bound| bound.touched.elapsed() < Duration::from_secs(300))
            .map(|bound| bound.association.clone())
    }
    pub(super) fn observation(
        &self,
        association: ReturnAssociation,
    ) -> Arc<dyn zero_traits::IoObserver> {
        let observation = Arc::new(Observation {
            observer: self.observer.clone(),
            association,
        });
        if self.state.lock().unwrap().lost {
            observation.record(PacketIoFact::ReceiveCoverageLost);
        }
        observation
    }
}
struct Observation {
    observer: Option<Arc<dyn ConnectObserver>>,
    association: ReturnAssociation,
}
impl std::fmt::Debug for Observation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PacketConversationObservation")
    }
}
impl Observation {
    fn record(&self, fact: PacketIoFact) {
        if let Some(observer) = &self.observer {
            observer.packet_io(&self.association, fact);
        }
    }
}
impl zero_traits::IoObserver for Observation {
    fn received(&self, bytes: usize) {
        self.record(PacketIoFact::Received { bytes });
    }
    fn sent(&self, bytes: usize) {
        self.record(PacketIoFact::Sent { bytes });
    }
    fn error(&self) {
        self.record(PacketIoFact::Error);
    }
    fn dropped(&self) {
        self.dropped_reason(zero_traits::PacketDropReason::Unspecified);
    }
    fn dropped_reason(&self, reason: zero_traits::PacketDropReason) {
        self.record(PacketIoFact::Dropped { bytes: 0, reason });
    }
    fn receive_coverage_lost(&self) {
        self.record(PacketIoFact::ReceiveCoverageLost);
    }
}

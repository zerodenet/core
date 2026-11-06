//! Same native reply association and original endpoint-generation routing.
use super::*;
use std::{
    collections::HashMap,
    net::IpAddr,
    time::{Duration, Instant},
};
use zero_stack::packet::{self, PacketConversationKey};
const MAX_ROUTES: usize = 1024;
const MAX_CONVERSATIONS: usize = 4096;
const ROUTE_IDLE: Duration = Duration::from_secs(300);
const CONVERSATION_IDLE: Duration = Duration::from_secs(600);
#[derive(Default)]
pub(in crate::runtime::packet_route::host) struct Returns {
    state: Mutex<State>,
}
#[derive(Default)]
struct State {
    routes: HashMap<IpAddr, Route>,
    conversations: HashMap<PacketConversationKey, Route>,
}
#[derive(Clone)]
struct Route {
    session: Weak<Session>,
    association: ReturnAssociation,
    touched: Instant,
}
impl Returns {
    pub(super) fn register(
        &self,
        packet: &IpPacket,
        session: &Arc<Session>,
    ) -> Result<(), ChannelError> {
        let source = packet::ip_source(packet.payload())
            .ok_or_else(|| error(Reason::Protocol, "native Packet has no IP source"))?;
        let key = packet::packet_conversation_key(packet.payload());
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let live = |route: &Route| {
            route
                .session
                .upgrade()
                .filter(|view| !view.logical_closed())
        };
        if let Some(current) = state.routes.get(&source).and_then(live) {
            if !current.same_source(session) {
                return Err(error(
                    Reason::Capacity,
                    "native Packet source belongs to another original ingress",
                ));
            }
            if state.routes[&source].association.peer != packet.return_association.peer {
                return Err(error(
                    Reason::InvalidConfig,
                    "same native source IP belongs to another authenticated peer",
                ));
            }
        }
        if let Some(route) = key.and_then(|key| state.conversations.get(&key)) {
            if let Some(current) = live(route) {
                if !current.same_source(session) || route.association != packet.return_association {
                    return Err(error(
                        Reason::StalePlan,
                        "native Packet conversation retains its original source association",
                    ));
                }
            }
        }
        state.routes.retain(|_, route| live(route).is_some());
        state.conversations.retain(|_, route| live(route).is_some());
        if !state.routes.contains_key(&source) && state.routes.len() >= MAX_ROUTES {
            return Err(error(
                Reason::Capacity,
                "native Packet return route limit reached",
            ));
        }
        if key.is_some_and(|key| {
            !state.conversations.contains_key(&key)
                && state.conversations.len() >= MAX_CONVERSATIONS
        }) {
            return Err(error(
                Reason::Capacity,
                "native Packet observation limit reached",
            ));
        }
        let route = Route {
            session: Arc::downgrade(session),
            association: packet.return_association.clone(),
            touched: Instant::now(),
        };
        state
            .routes
            .entry(source)
            .and_modify(|route| route.touched = Instant::now())
            .or_insert_with(|| route.clone());
        if let Some(key) = key {
            state
                .conversations
                .entry(key)
                .and_modify(|route| route.touched = Instant::now())
                .or_insert(route);
        }
        Ok(())
    }
    pub(in crate::runtime::packet_route::host) fn deliver_owned(
        &self,
        mut packet: zero_traits::PacketBuffer,
    ) -> Result<(), zero_traits::PacketBuffer> {
        let Some(destination) = packet::ip_destination(&packet) else {
            return Err(packet);
        };
        let key = packet::packet_return_key(&packet);
        let route = {
            let state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            key.and_then(|key| state.conversations.get(&key))
                .filter(|route| route.touched.elapsed() < CONVERSATION_IDLE)
                .or_else(|| {
                    state
                        .routes
                        .get(&destination)
                        .filter(|route| route.touched.elapsed() < ROUTE_IDLE)
                })
                .cloned()
        };
        let Some(route) = route else {
            return Err(packet);
        };
        let Some(session) = route.session.upgrade() else {
            return Ok(());
        };
        if !packet::advance_ip_hop(&mut packet) {
            return Ok(());
        }
        if let Ok(packet) = IpPacket::new(packet, route.association) {
            let _ = session.deliver(packet, true);
        }
        Ok(())
    }
    pub(in crate::runtime::packet_route::host) fn expire(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Idle timing limits receive attribution, not source ownership. An
        // active source cannot silently lose its overlap exclusion merely
        // because it has not submitted a frame recently.
        state.routes.retain(|_, route| {
            route
                .session
                .upgrade()
                .is_some_and(|session| !session.logical_closed())
        });
        state.conversations.retain(|_, route| {
            route.touched.elapsed() < CONVERSATION_IDLE
                && route
                    .session
                    .upgrade()
                    .is_some_and(|session| !session.logical_closed())
        });
    }
    pub(in crate::runtime::packet_route::host) fn clear(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.routes.clear();
        state.conversations.clear();
    }
}

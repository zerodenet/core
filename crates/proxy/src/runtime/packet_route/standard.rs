//! The existing source main owns bounded L3 sessions and polls their real RX.
//! This is transferred logical custody, without a receive pump or child task.
mod entry;
mod observation;
use crate::runtime::{
    inbound_owners::InboundTaskAccess, route_runtime::InboundRouteRuntimeFactory,
};
use entry::Entry;
use std::{
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};
use tokio::time::Instant;
use zero_core::{
    component::RoleInstanceId,
    packet::{IpFamily, IpPacket, PacketFacts},
};
use zero_platform_tokio::contract::{
    lifecycle::{
        erase_lifecycle, CloseReport, DynResourceLifecycle, ResourceLifecycle, Unfinished,
    },
    packet::{OwnedSendFailure, PacketLimits, PacketRequest, ReturnGuarantee, SourceAdaptation},
    CancellationToken, ChannelError, CloseReason, DeliveryState, Impact, Reason, SendFailure,
    Stage,
};

// Same actual Source conversation ceiling; this set owns no extra workers.
const MAX_SESSIONS: usize = 4_096;
type Key = (RoleInstanceId, bool);
#[derive(Clone, Default)]
pub(crate) struct PacketRoleSessions(Arc<Inner>);
#[derive(Default)]
struct Inner {
    state: Mutex<State>,
    opening: tokio::sync::Mutex<()>,
}
#[derive(Default)]
struct State {
    entries: Vec<Arc<Entry>>,
    next: usize,
    receiver: Option<Waker>,
    closed: bool,
}
impl PacketRoleSessions {
    pub(crate) fn resource(&self) -> Box<dyn DynResourceLifecycle> {
        erase_lifecycle(self.clone())
    }
    /// Admission precedes the first owned submission. The exact selected role
    /// generation and actual source facts are never reconstructed from a tag.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn send(
        &self,
        selected: RoleInstanceId,
        translated: bool,
        facts: Arc<PacketFacts>,
        return_mtu: u16,
        packet: IpPacket,
        route: &InboundRouteRuntimeFactory,
        source: &InboundTaskAccess,
        inner: Option<Arc<dyn zero_traits::IoObserver>>,
    ) -> Result<Option<Arc<str>>, OwnedSendFailure> {
        let key = (selected, translated);
        let _opening = self.0.opening.lock().await;
        let existing = {
            let state = self.0.state.lock().unwrap();
            if state.closed {
                return Err(rejected(packet, Reason::Revoked));
            }
            state
                .entries
                .iter()
                .find(|entry| entry.key == key && !entry.retiring())
                .cloned()
        };
        let entry = match existing {
            Some(entry) => entry,
            None => {
                if self.0.state.lock().unwrap().entries.len() >= MAX_SESSIONS {
                    return Err(rejected(packet, Reason::Capacity));
                }
                let cancel = CancellationToken::new();
                let observation = Arc::new(observation::Observation::new(inner));
                let session = match route
                    .open_packet_role(
                        selected,
                        facts.clone(),
                        PacketRequest {
                            endpoint: facts.endpoint,
                            return_mtu: Some(return_mtu),
                            limits: PacketLimits {
                                mtu: 65_535,
                                families: vec![IpFamily::Ipv4, IpFamily::Ipv6],
                                source_adaptation: if translated {
                                    SourceAdaptation::ValidatedTranslation
                                } else {
                                    SourceAdaptation::Preserved
                                },
                                return_association: ReturnGuarantee::Endpoint,
                            },
                        },
                        source,
                        cancel.clone(),
                        Some(observation.clone()),
                    )
                    .await
                {
                    Ok(session) => session,
                    Err(error) => {
                        return Err(OwnedSendFailure {
                            failure: Box::new(SendFailure::new(error, DeliveryState::NotSubmitted)),
                            packet: Some(packet),
                        })
                    }
                };
                let entry = Arc::new(Entry::new(key, session, cancel, observation));
                let receiver = {
                    let mut state = self.0.state.lock().unwrap();
                    if state.closed {
                        entry.request_stop();
                    }
                    // Even an interrupted start remains owned until this
                    // original controller and its actual views are closed.
                    state.entries.push(entry.clone());
                    state.receiver.take()
                };
                if let Some(receiver) = receiver {
                    receiver.wake();
                }
                entry
            }
        };
        drop(_opening);
        entry.send(packet).await
    }
    /// `None` is a real retired-role event; it requests inline logical cleanup.
    pub(crate) async fn next(&self) -> Option<PacketRoleReply> {
        std::future::poll_fn(|cx| self.poll_next(cx)).await
    }
    fn poll_next(&self, cx: &mut Context<'_>) -> Poll<Option<PacketRoleReply>> {
        let mut state = self.0.state.lock().unwrap();
        state.receiver = Some(cx.waker().clone());
        let count = state.entries.len();
        for offset in 0..count {
            let index = (state.next + offset) % count;
            let result = state.entries[index].poll_recv(cx);
            if result.is_ready() {
                state.next = (index + 1) % count;
                return result
                    .map(|packet| packet.map(|packet| state.entries[index].reply(packet)));
            }
        }
        Poll::Pending
    }
    /// Always polled by the same main, even while its original native sender is
    /// Pending. Observing retirement never drains or copies a received packet.
    pub(crate) async fn retired(&self) {
        std::future::poll_fn(|cx| {
            let state = self.0.state.lock().unwrap();
            for entry in &state.entries {
                if entry.poll_retired(cx).is_ready() {
                    return Poll::Ready(());
                }
            }
            Poll::Pending
        })
        .await
    }
    pub(crate) async fn close_retired(&self, deadline: Instant) -> Result<(), ChannelError> {
        let entries = self.0.state.lock().unwrap().entries.clone();
        let mut failure = None;
        for entry in entries {
            if entry.retiring() {
                if !entry.close(deadline).await? {
                    return Err(error(Reason::Timeout));
                }
                if failure.is_none() {
                    failure = entry.failure();
                }
                self.0
                    .state
                    .lock()
                    .unwrap()
                    .entries
                    .retain(|actual| !Arc::ptr_eq(actual, &entry));
            }
        }
        failure.map_or(Ok(()), |source| {
            Err(crate::component::instance::retained(source))
        })
    }
    /// Called only by this collection's real Source main after its admitted
    /// pins, children, native obligations and queued sends are actually idle.
    /// Retire cached logical views, then consume their real controllers. The
    /// collection stays open so a resumed Source may acquire a fresh session.
    pub(crate) async fn close_idle(&self, deadline: Instant) -> Result<(), ChannelError> {
        let _opening = self.0.opening.lock().await;
        let entries = self.0.state.lock().unwrap().entries.clone();
        for entry in entries {
            entry.request_stop();
        }
        self.close_retired(deadline).await
    }
    fn stop(&self) {
        let entries = {
            let mut state = self.0.state.lock().unwrap();
            state.closed = true;
            state.entries.clone()
        };
        for entry in entries {
            entry.request_stop();
        }
    }
}
impl ResourceLifecycle for PacketRoleSessions {
    async fn start(&mut self, _: Instant) -> Result<(), ChannelError> {
        if self.0.state.lock().unwrap().closed {
            Err(error(Reason::Revoked))
        } else {
            Ok(())
        }
    }
    fn revoke(&self, _: CloseReason) {
        self.stop();
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.stop();
        self.close_retired(deadline).await?;
        let mut report = CloseReport::default();
        if !self.0.state.lock().unwrap().entries.is_empty() {
            report.unfinished.push(Unfinished::Io(
                "original Packet role sessions remain retained".into(),
            ));
        }
        Ok(report)
    }
}
fn rejected(packet: IpPacket, reason: Reason) -> OwnedSendFailure {
    OwnedSendFailure {
        failure: Box::new(SendFailure::new(error(reason), DeliveryState::NotSubmitted)),
        packet: Some(packet),
    }
}
fn error(reason: Reason) -> ChannelError {
    ChannelError::new(Stage::Send, reason, Impact::Packet)
}

/// The original reply and its original observation travel together. Generic
/// routing never reconstructs or rewrites the opaque association.
pub(crate) struct PacketRoleReply {
    pub(crate) packet: IpPacket,
    observation: Arc<observation::Observation>,
}
impl PacketRoleReply {
    pub(crate) fn into_packet(self) -> IpPacket {
        self.packet
    }
    pub(crate) fn discard_policy(self) {
        self.observation.discard(&self.packet);
    }
}

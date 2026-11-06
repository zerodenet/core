//! One bounded native reply sink per actual peer in this logical source session.
//! A return cookie identifies its installed source/conversation, not a request
//! packet pairing. Each submitted IpPacket retains its own original cookie.
mod rx;
mod state;
mod tx;
use super::*;
use state::{Shared, State};
use std::{pin::Pin, sync::Mutex};
use zero_core::packet::{IpFamily, PacketFacts};
use zero_platform_tokio::contract::{
    lifecycle::{erase_control, ChannelControl, CloseReport},
    packet::{
        BoxPacketSession, DynPacketOutbound, PacketLimits, PacketRequest, PacketSession,
        ReturnGuarantee, SourceAdaptation,
    },
    BoxFuture, CancellationToken,
};
impl DynPacketOutbound for Carrier {
    fn open(
        &self,
        request: PacketRequest,
        context: OpenContext,
    ) -> BoxFuture<'_, Result<BoxPacketSession, ChannelError>> {
        Box::pin(async move {
            let return_mtu = request
                .return_mtu
                .filter(|mtu| *mtu != 0)
                .ok_or_else(|| error(Stage::Prepare, Reason::InvalidConfig))?;
            let facts = context.packet_facts()?.clone();
            if request.endpoint != facts.endpoint {
                return Err(error(Stage::Prepare, Reason::InvalidConfig));
            }
            let acquiring = self.acquisition()?;
            let carrier = self.handle()?;
            let caller = self.flow_borrow(&context)?.caller;
            let users = Arc::new(state::ViewUsers::default());
            let shared = Arc::new(Shared {
                caller,
                users: Arc::downgrade(&users),
                carrier,
                facts,
                return_mtu,
                observer: context.observer,
                translate: request.limits.source_adaptation
                    == SourceAdaptation::ValidatedTranslation,
                cancel: context.cancel.child_token(),
                state: Mutex::new(State::default()),
            });
            let session = PacketSession {
                tx: Box::pin(tx::Tx {
                    shared: shared.clone(),
                    _users: users.clone(),
                })
                    as Pin<Box<dyn zero_platform_tokio::contract::packet::PacketTx>>,
                rx: Box::pin(rx::Rx {
                    shared: shared.clone(),
                    _users: users,
                    cancelled: Box::pin(shared.cancel.clone().cancelled_owned()),
                })
                    as Pin<Box<dyn zero_platform_tokio::contract::packet::PacketRx>>,
                control: erase_control(Control(shared)),
                limits: PacketLimits {
                    // The concrete forwarding stack performs MTU errors and
                    // real IPv4 fragmentation; envelope limits must admit it.
                    mtu: 65_535,
                    families: self
                        .plan()
                        .local_addresses()
                        .iter()
                        .map(|address| {
                            if address.is_ipv4() {
                                IpFamily::Ipv4
                            } else {
                                IpFamily::Ipv6
                            }
                        })
                        .collect(),
                    source_adaptation: request.limits.source_adaptation,
                    return_association: ReturnGuarantee::Endpoint,
                },
            };
            drop(acquiring);
            Ok(session)
        })
    }
}
struct Control(Arc<Shared>);
impl ChannelControl for Control {
    fn cancel(&self, _: zero_platform_tokio::contract::CloseReason) {
        self.0.stop();
    }
    async fn wait_closed(&mut self, _: tokio::time::Instant) -> Result<CloseReport, ChannelError> {
        self.0.stop();
        // No internal task exists for this view. Stack/wire/native shutdown is
        // confirmed only by the original device host, never by this close.
        Ok(CloseReport::default())
    }
}

//! One standard Packet session for one actual endpoint; no business identity.
use super::*;
use std::{
    sync::Mutex,
    task::{Context, Poll},
};
use tokio::time::Instant;
use zero_core::{
    component::*,
    packet::{IpPacket, PacketFacts},
};
use zero_platform_tokio::contract::{
    component::*,
    context::{AcceptContext, HandshakeContext},
    inbound::*,
    lifecycle::*,
    packet::*,
    scope::{TaskClass, TaskJoinWitness, TaskRegistrar},
    ChannelError, CloseReason, DeliveryState, Impact, Reason, SendFailure, SendReceipt, Stage,
};
use zero_platform_tokio::PollSender;
mod channel;
use channel::{Control, Rx, Tx};
struct Shared {
    state: Mutex<State>,
    stop: watch::Sender<bool>,
    cancel: zero_platform_tokio::contract::CancellationToken,
    tasks: TaskRegistrar,
    proxies: tasks::RetainedTasks<()>,
    done: Mutex<Option<TaskJoinWitness>>,
}
struct State {
    wire: Option<wire::Wire>,
    grant: Option<GrantedDatagramListener>,
    taken: bool,
}
pub(crate) struct Owner {
    shared: Arc<Shared>,
    resources: Arc<dyn ResourceAccess>,
    destination: zero_core::request::Destination,
}
pub(crate) struct Source {
    shared: Arc<Shared>,
    identity: ComponentInstanceId,
    inbound: InboundId,
}
pub(crate) fn source(
    device: Box<dyn RawIpInboundDevice>,
    endpoint: Option<RawIpInboundEndpoint>,
    destination: zero_core::request::Destination,
    context: BuildContext,
) -> (Owner, Source) {
    let identity = context.instance();
    let inbound = context.roles.inbound.expect("actual inbound role");
    let (stop, _) = watch::channel(false);
    let proxies = tasks::RetainedTasks::new(
        "raw-IP outer carrier readers joined",
        crate::runtime::raw_ip::MAX_RAW_IP_DEVICES,
    );
    let shared = Arc::new(Shared {
        state: Mutex::new(State {
            wire: Some(wire::Wire {
                device,
                endpoint,
                proxies: proxies.clone(),
            }),
            grant: None,
            taken: false,
        }),
        stop,
        cancel: context.cancel.child_token(),
        tasks: context.tasks,
        proxies,
        done: Mutex::new(None),
    });
    (
        Owner {
            shared: shared.clone(),
            resources: context.resources,
            destination,
        },
        Source {
            shared,
            identity,
            inbound,
        },
    )
}
fn error(reason: Reason) -> ChannelError {
    ChannelError::new(Stage::Receive, reason, Impact::Component)
}
impl ResourceLifecycle for Owner {
    async fn start(&mut self, _: Instant) -> Result<(), ChannelError> {
        let grant = self
            .resources
            .listen_datagram(self.destination.clone())
            .await?;
        self.shared.state.lock().unwrap().grant = Some(grant);
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {
        self.shared.cancel.cancel();
        self.shared.stop.send_replace(true);
        self.shared.proxies.request_abort();
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.revoke(CloseReason::Shutdown);
        let witness = self.shared.done.lock().unwrap().clone();
        if let Some(witness) = witness {
            tokio::time::timeout_at(deadline, witness.wait())
                .await
                .map_err(|source| error(Reason::Timeout).with_source(source))?;
        }
        // The real Scope join above ends wire custody. Native close belongs to
        // the original listener provider, not this logical-session report.
        self.shared.state.lock().unwrap().grant.take();
        self.shared.state.lock().unwrap().wire.take();
        self.shared.proxies.clone().shutdown(deadline).await
    }
}
impl InboundSource for Source {
    type Handshake = PendingInbound;
    async fn next(
        &mut self,
        context: AcceptContext,
    ) -> Result<Option<PendingInbound>, ChannelError> {
        if self.shared.cancel.is_cancelled() || context.cancel.is_cancelled() {
            return Ok(None);
        }
        if self.shared.state.lock().unwrap().taken {
            tokio::select! { _=self.shared.cancel.cancelled()=>return Ok(None),_=context.cancel.cancelled()=>return Ok(None) }
        }
        let mut state = self.shared.state.lock().unwrap();
        let original = state
            .grant
            .as_ref()
            .ok_or_else(|| error(Reason::Internal))?
            .tx
            .identity();
        state.taken = true;
        let facts = PacketFacts {
            endpoint: EndpointId(self.identity.component.0),
            inbound: RoleInstanceId {
                component: self.identity.component,
                role: RoleId::Inbound(self.inbound),
                generation: self.identity.generation,
            },
            carrier_source: None,
            subject: None,
            peer: None,
        };
        Ok(Some(PendingInbound::from_packet_listener(
            Handshake {
                shared: self.shared.clone(),
                facts,
            },
            original,
        )))
    }
}
struct Handshake {
    shared: Arc<Shared>,
    facts: PacketFacts,
}
impl InboundHandshake for Handshake {
    async fn complete(self, context: HandshakeContext) -> Result<AcceptedRequest, ChannelError> {
        if context.inbound != self.facts.inbound || context.accepted.is_some() {
            return Err(error(Reason::InvalidConfig));
        }
        Ok(AcceptedRequest::Packet(accept_packet(Reply {
            shared: self.shared,
            facts: self.facts,
        })))
    }
}
struct Reply {
    shared: Arc<Shared>,
    facts: PacketFacts,
}
impl PacketReply for Reply {
    fn link_mtu(&self) -> Option<u16> {
        self.shared
            .state
            .lock()
            .unwrap()
            .wire
            .as_ref()
            .map(|wire| wire.device.mtu())
    }
    type Tx = Tx;
    type Rx = Rx;
    type Control = Control;
    fn facts(&self) -> &PacketFacts {
        &self.facts
    }
    async fn respond(self, _: ReplyOutcome) -> PacketReplyResult<Tx, Rx, Control> {
        Err(error(Reason::Unsupported))
    }
    async fn respond_observed(
        self,
        outcome: ReplyOutcome,
        observer: Arc<dyn PacketObserver>,
    ) -> PacketReplyResult<Tx, Rx, Control> {
        if outcome != ReplyOutcome::LocalChannelReady {
            self.shared.stop.send_replace(true);
            return Ok(None);
        }
        let (wire, grant) = {
            let mut state = self.shared.state.lock().unwrap();
            (
                state.wire.take().ok_or_else(|| error(Reason::Revoked))?,
                state.grant.take().ok_or_else(|| error(Reason::Revoked))?,
            )
        };
        let mtu = wire.device.mtu();
        let (packets, received) = mpsc::channel(1);
        let (responses, returned) = mpsc::channel(1);
        let cancel = self.shared.cancel.clone();
        let shutdown = self.shared.stop.subscribe();
        let egress = observer.egress();
        let proxies = self.shared.proxies.clone();
        let guard = cancel.clone().drop_guard();
        let facts = self.facts;
        let (_id, witness) = self
            .shared
            .tasks
            .spawn_tracked(TaskClass::Critical, async move {
                let _guard = guard;
                let result = wire::run(
                    wire, grant, egress, packets, returned, facts, observer, shutdown,
                )
                .await;
                proxies.request_abort();
                result.map_err(|source| error(Reason::Io).with_source(source))
            })
            .map_err(|source| error(Reason::Capacity).with_source(source))?;
        *self.shared.done.lock().unwrap() = Some(witness.clone());
        Ok(Some(PacketSession {
            tx: Tx {
                sender: PollSender::new(responses),
                cancel: cancel.clone(),
            },
            rx: Rx { receiver: received },
            control: Control {
                cancel,
                stop: self.shared.stop.clone(),
                witness,
            },
            limits: PacketLimits {
                mtu: usize::from(mtu).max(65535),
                families: vec![
                    zero_core::packet::IpFamily::Ipv4,
                    zero_core::packet::IpFamily::Ipv6,
                ],
                source_adaptation: SourceAdaptation::Preserved,
                return_association: ReturnGuarantee::Peer,
            },
        }))
    }
}

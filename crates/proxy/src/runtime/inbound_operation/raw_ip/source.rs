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
    pending_tx: Arc<Mutex<Option<IpPacket>>>,
    resources: Arc<dyn ResourceAccess>,
}
struct State {
    wire: Option<wire::Wire>,
    grant: Option<GrantedDatagramListener>,
    taken: bool,
    reads: Option<[zero_tun::packet_memory::NativeReadGrant; 2]>,
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
            reads: None,
        }),
        stop,
        cancel: context.cancel.child_token(),
        tasks: context.tasks,
        proxies,
        done: Mutex::new(None),
        pending_tx: Arc::new(Mutex::new(None)),
        resources: context.resources.clone(),
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
        #[cfg(test)]
        eprintln!(
            "actual Packet Source lifecycle before read-grant take: {:?}",
            self.destination
        );
        let reads = self.resources.take_packet_datagram_reads(0)?;
        #[cfg(test)]
        eprintln!(
            "actual Packet Source lifecycle read-grants taken, before listener: {:?}",
            self.destination
        );
        self.shared.state.lock().unwrap().reads = Some(reads);
        let grant = self
            .resources
            .listen_datagram(self.destination.clone())
            .await?;
        self.shared.state.lock().unwrap().grant = Some(grant);
        #[cfg(test)]
        eprintln!(
            "actual Packet Source lifecycle retained listener: {:?}",
            self.destination
        );
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
        self.shared.pending_tx.lock().unwrap().take();
        // The real Scope join above ends wire custody. Native close belongs to
        // the original listener provider, not this logical-session report.
        self.shared.state.lock().unwrap().grant.take();
        self.shared.state.lock().unwrap().wire.take();
        let reads = self.shared.state.lock().unwrap().reads.take();
        if let Some(reads) = reads {
            for read in reads {
                read.revoke();
            }
        }
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
        #[cfg(test)]
        eprintln!(
            "actual Packet Source respond before wire custody: {:?}",
            self.facts.inbound
        );
        let (wire, grant, reads) = {
            let mut state = self.shared.state.lock().unwrap();
            (
                state.wire.take().ok_or_else(|| error(Reason::Revoked))?,
                state.grant.take().ok_or_else(|| error(Reason::Revoked))?,
                state.reads.take().ok_or_else(|| error(Reason::Revoked))?,
            )
        };
        let original = grant.tx.identity();
        let (registered, original_registration) = tokio::sync::oneshot::channel::<()>();
        let (checkpoint_sender, actual_checkpoint) = tokio::sync::oneshot::channel();
        let mtu = wire.device.mtu();
        let (packets, received) = mpsc::channel(1);
        let (responses, returned) = mpsc::channel(1);
        let cancel = self.shared.cancel.clone();
        let shutdown = self.shared.stop.subscribe();
        let egress = observer.egress();
        let proxies = self.shared.proxies.clone();
        let guard = cancel.clone().drop_guard();
        let waiting_cancel = cancel.clone();
        let source_facts = self.facts.clone();
        let facts = self.facts;
        let (_id, witness) = self
            .shared
            .tasks
            .spawn_tracked(TaskClass::Critical, async move {
                let _guard = guard;
                // This is the same real wire registration. It owns the unique
                // views while waiting; no IO begins before its retained native
                // controller has validated and saved this exact witness.
                tokio::select! {
                    biased;
                    _ = waiting_cancel.cancelled() => return Ok(()),
                    ready = original_registration => ready.map_err(|source| error(Reason::Revoked).with_source(source))?,
                }
                let checkpoint=tokio::select! {
                    biased;
                    _=waiting_cancel.cancelled()=>return Ok(()),
                    checkpoint=actual_checkpoint=>checkpoint.map_err(|source|error(Reason::Revoked).with_source(source))?,
                };
                let result = wire::run(
                    wire, grant, reads, egress, packets, returned, facts, observer, shutdown, checkpoint,
                )
                .await;
                proxies.request_abort();
                result.map_err(|source| error(Reason::Io).with_source(source))
            })
            .map_err(|source| error(Reason::Capacity).with_source(source))?;
        #[cfg(test)]
        eprintln!(
            "actual Packet Source wire registered: {:?}, task {:?}",
            source_facts.inbound,
            witness.task_id()
        );
        *self.shared.done.lock().unwrap() = Some(witness.clone());
        let registered_driver = self
            .shared
            .resources
            .register_packet_source_driver(&original, &witness)?;
        #[cfg(test)]
        eprintln!(
            "actual Packet Source native driver registration accepted: {:?}",
            source_facts.inbound
        );
        let checkpoint = registered_driver.checkpoint;
        checkpoint_sender
            .send(checkpoint)
            .map_err(|_| error(Reason::Revoked))?;
        registered.send(()).map_err(|_| error(Reason::Revoked))?;
        #[cfg(test)]
        eprintln!(
            "actual Packet Source wire original gate released: {:?}",
            source_facts.inbound
        );
        Ok(Some(PacketSession {
            tx: Tx {
                sender: PollSender::new(responses),
                cancel: cancel.clone(),
                pending: self.shared.pending_tx.clone(),
                facts: source_facts,
            },
            rx: Rx { receiver: received },
            control: Control {
                cancel,
                stop: self.shared.stop.clone(),
                witness,
                pending: self.shared.pending_tx.clone(),
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

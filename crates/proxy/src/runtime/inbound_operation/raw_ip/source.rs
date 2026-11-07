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
mod owner;
mod reply;
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
    outer_network: Option<std::sync::Weak<crate::component::background::BackgroundNetwork>>,
}
struct State {
    wire: Option<wire::Wire>,
    grant: Option<GrantedDatagramListener>,
    taken: bool,
    memory: Option<PacketSourceMemory>,
    reads: Option<[zero_tun::packet_memory::NativeReadGrant; 2]>,
    control: Option<zero_tun::packet_memory::NativeReadGrant>,
    wire_launch: Option<zero_platform_tokio::contract::scope::TaskLaunch>,
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
    outer_network: Option<std::sync::Weak<crate::component::background::BackgroundNetwork>>,
) -> Result<(Owner, Source), ChannelError> {
    let identity = context.instance();
    let inbound = context.roles.inbound.expect("actual inbound role");
    let (stop, _) = watch::channel(false);
    let proxies = tasks::RetainedTasks::new(
        "raw-IP outer carrier readers joined",
        crate::runtime::raw_ip::MAX_RAW_IP_DEVICES,
        TaskClass::Auxiliary,
    );
    proxies.bind(context.tasks.downgrade())?;
    let shared = Arc::new(Shared {
        state: Mutex::new(State {
            wire: Some(wire::Wire {
                device,
                endpoint,
                proxies: proxies.clone(),
                carriers: Vec::new(),
                proxy_reads: Vec::new(),
                proxy_codecs: Vec::new(),
            }),
            grant: None,
            taken: false,
            memory: None,
            reads: None,
            control: None,
            wire_launch: None,
        }),
        stop,
        cancel: context.cancel.child_token(),
        tasks: context.tasks,
        proxies,
        done: Mutex::new(None),
        pending_tx: Arc::new(Mutex::new(None)),
        resources: context.resources.clone(),
        outer_network,
    });
    Ok((
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
    ))
}
fn error(reason: Reason) -> ChannelError {
    ChannelError::new(Stage::Receive, reason, Impact::Component)
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

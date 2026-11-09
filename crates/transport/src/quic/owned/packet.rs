//! A persistent bounded Quinn wire view, with independently retired original
//! ancillary I/O workers. Logical QUIC users never borrow a native hop's views.
use super::{callbacks, DriverBody, OwnedDriverOrigin};
use crate::datagram_queue::{self, Channels};
use std::{
    io,
    net::SocketAddr,
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};
use zero_platform_tokio::{
    contract::{
        component::GrantedMetadataListener, scope::TaskRegistrar, CancellationToken, ChannelError,
        Impact, Reason, Stage,
    },
    controlled_datagram::metadata::users::{MetadataUserGraph, MetadataUsersClosedReceipt},
};

pub struct PacketSocket {
    identity: Arc<continuation::PacketIdentity>,
    pub(super) socket: Arc<dyn quinn::AsyncUdpSocket>,
    pub(super) callbacks: Arc<callbacks::Broker>,
    memory: Option<Arc<datagram_queue::memory::Shared>>,
}
mod borrowed;
mod continuation;
mod retire;
pub use borrowed::{
    BorrowedPacketReturned, BorrowedPacketSegment, BorrowedProtocolReturned,
    RejectedBorrowedPacketSegment,
};
/// Unique queues returned by the actual native worker before the next hop.
/// A receiver cannot be copied or concurrently attached to two originals.
pub struct PacketQueues(Option<Channels>, Arc<continuation::PacketIdentity>);
pub struct PacketSegment {
    identity: Arc<continuation::PacketIdentity>,
    graph: MetadataUserGraph<OwnedDriverOrigin, DriverBody>,
    receiver: super::DriverExitReceiver,
    source: Option<super::HostedMetadataSocket>,
    queues: Arc<Mutex<Option<Channels>>>,
    driver: Option<zero_core::component::TaskId>,
    received: Option<super::DriverDelivery>,
    stop: CancellationToken,
    final_retirement: Arc<AtomicBool>,
    peer: SocketAddr,
    logical: SocketAddr,
    tasks: TaskRegistrar,
}
pub struct RejectedPacketSegment {
    pub error: io::Error,
    pub grant: GrantedMetadataListener,
    pub queues: PacketQueues,
}
impl PacketSocket {
    pub fn prepare_with_budget_for_masks(
        logical: SocketAddr,
        book: zero_platform_tokio::controlled_carrier::CarrierBufferBudget,
        plan: &crate::finalmask::owned::PreparedMasks,
    ) -> io::Result<(Self, PacketQueues)> {
        if !plan.matches_book(&book) {
            return Err(io::ErrorKind::PermissionDenied.into());
        }
        let (socket, queues) = Self::prepare_with_budget(logical, book, &[])?;
        if plan.masked() {
            socket
                .memory
                .as_ref()
                .expect("original prepaid queue")
                .force_individual_datagrams();
        }
        Ok((socket, queues))
    }
    /// HY2's existing stateless Salamander masks stay protocol-selected. No
    /// additional native worker or unowned mask task can be started here.
    pub fn with_stateless_masks(
        mut self,
        masks: &[crate::finalmask::udp::Mask],
    ) -> io::Result<Self> {
        if self.memory.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "owned packet masks must be prepaid with their original queue",
            ));
        }
        super::HostedMetadataSocket::validate_stateless_masks(masks)?;
        self.socket = crate::finalmask::Socket::wrap(self.socket, masks, false)?;
        Ok(self)
    }
    /// Explicit standalone queue, also used by the native/borrowed transport
    /// fixtures. Ordinary owning factories use their same Host byte book via
    /// `prepare_with_budget`; missing Host resources never select this path.
    pub fn prepare(logical: SocketAddr) -> io::Result<(Self, PacketQueues)> {
        let callbacks = callbacks::Broker::new(1)?;
        let local = SocketAddr::new(
            if logical.is_ipv4() {
                std::net::Ipv4Addr::UNSPECIFIED.into()
            } else {
                std::net::Ipv6Addr::UNSPECIFIED.into()
            },
            0,
        );
        let (socket, channels) = datagram_queue::prepare_channels(
            local,
            Arc::new(move |packet| {
                if packet.destination != logical {
                    return Err(io::ErrorKind::InvalidInput.into());
                }
                Ok(packet.contents.to_vec())
            }),
        );
        let identity = Arc::new(continuation::PacketIdentity::default());
        Ok((
            Self {
                socket,
                callbacks,
                memory: None,
                identity: identity.clone(),
            },
            PacketQueues(Some(channels), identity),
        ))
    }
    /// Actual owning factory path. Its original Root prepays the minimum
    /// scratch/codec/copy working set before opening native resources.
    pub fn prepare_with_budget(
        logical: SocketAddr,
        book: zero_platform_tokio::controlled_carrier::CarrierBufferBudget,
        masks: &[crate::finalmask::udp::Mask],
    ) -> io::Result<(Self, PacketQueues)> {
        let memory = datagram_queue::memory::Memory::prepare(book, masks)?;
        let shared = memory.shared.clone();
        let callbacks = callbacks::Broker::new(1)?;
        let local = SocketAddr::new(
            if logical.is_ipv4() {
                std::net::Ipv4Addr::UNSPECIFIED.into()
            } else {
                std::net::Ipv6Addr::UNSPECIFIED.into()
            },
            0,
        );
        let (socket, channels) = datagram_queue::prepare_budgeted_channels(local, logical, memory);
        let identity = Arc::new(continuation::PacketIdentity::default());
        Ok((
            Self {
                identity: identity.clone(),
                socket,
                callbacks,
                memory: Some(shared),
            },
            PacketQueues(Some(channels), identity),
        ))
    }
}
impl PacketSegment {
    /// No worker starts here. The pool stores this owner before calling start.
    pub fn prepare(
        mut grant: GrantedMetadataListener,
        queues: PacketQueues,
        tasks: TaskRegistrar,
        peer: SocketAddr,
        logical: SocketAddr,
    ) -> Result<Self, Box<RejectedPacketSegment>> {
        let fail = |error, grant, queues| {
            Box::new(RejectedPacketSegment {
                error,
                grant,
                queues,
            })
        };
        let capabilities = grant.tx.capabilities();
        if !grant.scope.matches(&tasks) || !grant.scope.registration_open() {
            return Err(fail(io::ErrorKind::PermissionDenied.into(), grant, queues));
        }
        let Some(claim) = grant.users_claim.take() else {
            return Err(fail(
                io::Error::other("original packet claim missing"),
                grant,
                queues,
            ));
        };
        if !claim.matches(&grant.rx, &grant.tx) {
            grant.users_claim = Some(claim);
            return Err(fail(
                io::Error::other("foreign packet views"),
                grant,
                queues,
            ));
        }
        if let Some(memory) = queues
            .0
            .as_ref()
            .and_then(|channels| channels.memory.as_ref())
        {
            // Real native GSO capability is known now. Reserve its complete
            // legal batch peak before any endpoint/packet driver can start.
            if let Err(source) = memory.shared.grow_native(capabilities.transmit_segments) {
                grant.users_claim = Some(claim);
                return Err(fail(source, grant, queues));
            }
        }
        let source = match super::HostedMetadataSocket::new(grant, &tasks) {
            Ok(source) => source,
            Err(rejected) => {
                let mut rejected = *rejected;
                rejected.grant.users_claim = Some(claim);
                return Err(fail(rejected.error, rejected.grant, queues));
            }
        };
        let (graph, receiver) = match claim.bind_users(
            tasks.clone(),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        ) {
            Ok(value) => value,
            Err(claim) => {
                return Err(fail(
                    io::ErrorKind::PermissionDenied.into(),
                    source.into_unstarted(claim),
                    queues,
                ));
            }
        };
        if let Some(native) = queues
            .0
            .as_ref()
            .and_then(|channels| channels.native.as_ref())
        {
            let mut current = native.lock().unwrap();
            current.current = Some(capabilities);
            current.maximum_transmit_segments = current
                .maximum_transmit_segments
                .max(capabilities.transmit_segments);
        }
        Ok(Self {
            identity: queues.1.clone(),
            graph,
            receiver,
            source: Some(source),
            queues: Arc::new(Mutex::new(queues.0)),
            driver: None,
            received: None,
            // Scope cancellation fences business and registration, but this
            // same ancillary original must remain available for Quinn's final
            // accepted transmissions until the endpoint drivers have joined.
            // This token controls cleanup only; it grants no native authority.
            stop: CancellationToken::new(),
            final_retirement: Arc::new(AtomicBool::new(false)),
            peer,
            logical,
            tasks,
        })
    }
    pub fn start(&mut self) -> Result<(), ChannelError> {
        if self.driver.is_some()
            || self.stop.is_cancelled()
            || self.tasks.cancellation().is_cancelled()
        {
            return Err(error(Reason::Revoked));
        }
        let source = self.source.take().ok_or_else(|| error(Reason::Revoked))?;
        let channels = self
            .queues
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| error(Reason::Revoked))?;
        let returned = self.queues.clone();
        let stop = self.stop.clone();
        let final_retirement = self.final_retirement.clone();
        let peer = self.peer;
        let logical = self.logical;
        let origin = OwnedDriverOrigin::Carrier;
        struct Return {
            channels: Option<Channels>,
            target: Arc<Mutex<Option<Channels>>>,
        }
        impl Drop for Return {
            fn drop(&mut self) {
                *self.target.lock().unwrap() = self.channels.take();
            }
        }
        let mut queue = Return {
            channels: Some(channels),
            target: returned,
        };
        let task=self.graph.drivers().spawn(origin,zero_platform_tokio::contract::scope::TaskClass::Auxiliary,move |slot| async move {
            let socket=source.socket();
            let callbacks=source.callbacks.clone();
            let work=Box::pin(async move {
                let channels=queue.channels.as_mut().unwrap();
                loop {
                    tokio::select! {biased;
                        _=stop.cancelled()=>{
                            // A final endpoint has returned all users and its
                            // actual drivers. Seal its original FIFO before
                            // draining. A live endpoint's UDP hop instead
                            // drains only the finite accepted prefix; later
                            // queued frames stay in these same returned queues.
                            if final_retirement.load(Ordering::Acquire) {
                                channels.outgoing.close();
                            }
                            let remaining=channels.outgoing.len();
                            for _ in 0..remaining {
                                let Some(mut packet)=channels.outgoing.recv().await else {break};
                                packet.peer=peer;
                                datagram_queue::send(&socket,packet).await?;
                            }
                            return Ok::<_,io::Error>(());
                        },
                        packet=channels.outgoing.recv()=>{
                            let Some(mut packet)=packet else{return Ok(())};packet.peer=peer;
                            // Keep this exact pending send in the original
                            // worker across retirement/deadline cancellation.
                            datagram_queue::send(&socket,packet).await?;
                            if let Some(native)=&channels.native {
                                let transmit=socket.max_transmit_segments();let fragment=socket.may_fragment();
                                let mut caps=native.lock().unwrap();
                                if let Some(current)=&mut caps.current {current.transmit_segments=transmit;current.may_fragment=fragment;}
                                caps.maximum_transmit_segments=caps.maximum_transmit_segments.max(transmit);
                            }
                        },
                        packets=async {
                            match channels.memory.as_mut() {
                                Some(memory) => datagram_queue::receive_budgeted(socket.as_ref(), &mut memory.receive, true).await,
                                None => datagram_queue::receive_native(socket.as_ref()).await,
                            }
                        }=>for mut packet in packets? {
                            if packet.peer==peer {packet.peer=logical;let _=channels.incoming.try_send(packet);}
                        },
                    }
                }
            });
            let result=callbacks::Boundary::new(work,callbacks).await;
            drop(source);
            let body=slot.record(DriverBody {origin,io_error:result.err().map(Arc::new),close_reason:None,
                #[cfg(any(feature="split_http",feature="hysteria",feature="quic_h3"))] http3:None});
            match &body.io_error {Some(source)=>Err(error(Reason::Io).with_source(Original(source.clone()))),None=>Ok(())}
        }).map_err(|source|error(Reason::Capacity).with_source(source))?;
        self.driver = Some(task);
        Ok(())
    }
    pub fn task_exit(&self) -> Option<&super::DriverDelivery> {
        self.received.as_ref()
    }
    pub fn tasks(&self) -> &TaskRegistrar {
        &self.tasks
    }
}
impl Drop for PacketSegment {
    fn drop(&mut self) {
        self.stop();
    }
}
fn error(reason: Reason) -> ChannelError {
    ChannelError::new(Stage::Close, reason, Impact::Component)
}
#[derive(Debug)]
struct Original(Arc<io::Error>);
impl std::fmt::Display for Original {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for Original {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

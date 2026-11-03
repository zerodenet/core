//! A persistent bounded Quinn wire view, with independently retired original
//! ancillary I/O workers. Logical QUIC users never borrow a native hop's views.
use super::{callbacks, DriverBody, OwnedDriverOrigin};
use crate::datagram_queue::{self, Channels};
use std::{
    io,
    net::SocketAddr,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};
use zero_platform_tokio::{
    contract::{
        component::GrantedMetadataListener, scope::TaskRegistrar, CancellationToken, ChannelError,
        Impact, Reason, Stage,
    },
    controlled_datagram::metadata::users::{MetadataUserGraph, MetadataUsersClosedReceipt},
};

pub struct PacketSocket {
    pub(super) socket: Arc<dyn quinn::AsyncUdpSocket>,
    pub(super) callbacks: Arc<callbacks::Broker>,
}
/// Unique queues returned by the actual native worker before the next hop.
/// A receiver cannot be copied or concurrently attached to two originals.
pub struct PacketQueues(Option<Channels>);
pub struct PacketSegment {
    graph: MetadataUserGraph<OwnedDriverOrigin, DriverBody>,
    receiver: super::DriverExitReceiver,
    source: Option<super::HostedMetadataSocket>,
    queues: Arc<Mutex<Option<Channels>>>,
    driver: Option<zero_core::component::TaskId>,
    received: Option<super::DriverDelivery>,
    stop: CancellationToken,
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
    /// HY2's existing stateless Salamander masks stay protocol-selected. No
    /// additional native worker or unowned mask task can be started here.
    pub fn with_stateless_masks(
        mut self,
        masks: &[crate::finalmask::udp::Mask],
    ) -> io::Result<Self> {
        super::HostedMetadataSocket::validate_stateless_masks(masks)?;
        self.socket = crate::finalmask::Socket::wrap(self.socket, masks, false)?;
        Ok(self)
    }
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
        Ok((Self { socket, callbacks }, PacketQueues(Some(channels))))
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
            graph,
            receiver,
            source: Some(source),
            queues: Arc::new(Mutex::new(queues.0)),
            driver: None,
            received: None,
            stop: tasks.cancellation().child_token(),
            peer,
            logical,
            tasks,
        })
    }
    pub fn start(&mut self) -> Result<(), ChannelError> {
        if self.driver.is_some() || self.stop.is_cancelled() {
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
                        _=stop.cancelled()=>return Ok::<_,io::Error>(()),
                        packet=channels.outgoing.recv()=>{
                            let Some(mut packet)=packet else{return Ok(())};packet.peer=peer;
                            tokio::select! {biased;_=stop.cancelled()=>return Ok(()),result=datagram_queue::send(&socket,packet)=>result?};
                            if let Some(native)=&channels.native {
                                let transmit=socket.max_transmit_segments();let fragment=socket.may_fragment();
                                let mut caps=native.lock().unwrap();
                                if let Some(current)=&mut caps.current {current.transmit_segments=transmit;current.may_fragment=fragment;}
                                caps.maximum_transmit_segments=caps.maximum_transmit_segments.max(transmit);
                            }
                        },
                        packets=datagram_queue::receive_native(socket.as_ref())=>for mut packet in packets? {
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
    pub fn stop(&self) {
        self.stop.cancel();
        self.graph.users().cancellation().cancel();
        self.graph.drivers().revoke_registration();
    }
    pub async fn retire(
        &mut self,
        end: tokio::time::Instant,
    ) -> Result<(PacketQueues, MetadataUsersClosedReceipt), ChannelError> {
        self.stop();
        drop(self.source.take());
        if let Some(id) = self.driver {
            if self.received.is_none() {
                self.received = Some(self.receiver.receive(id, end).await?);
            }
        }
        let receipt = self
            .graph
            .completion()
            .try_closed()
            .ok_or_else(|| error(Reason::Timeout))?;
        let channels = self
            .queues
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| error(Reason::Revoked))?;
        if let Some(native) = &channels.native {
            native.lock().unwrap().current = None;
        }
        Ok((PacketQueues(Some(channels)), receipt))
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

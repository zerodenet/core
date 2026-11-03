//! One actual UDP peer owns all conversation futures in its original host Scope.
use super::{
    peer::ListenerProfile,
    stream::{connection, Input},
    wire::{Body, Segment, TERMINATE},
    MkcpStream,
};
use crate::finalmask::udp::{Codec, Mask};
use futures_util::{stream::FuturesUnordered, StreamExt};
use std::{
    collections::HashMap,
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::sync::{mpsc, Mutex};
use zero_platform_tokio::{
    contract::{
        inbound::udp::{DatagramPeer, PeerRx, PeerTx},
        scope::TaskClass,
        BoxFuture, CancellationToken, ChannelError, Impact, Reason, Stage,
    },
    AcceptedCaller, AcceptedOrigin, PacketSocket,
};
use zero_traits::{PacketSocketIo, SocketAddress};

pub struct ScopedPeerStreams {
    streams: Mutex<mpsc::Receiver<MkcpStream>>,
    cancel: CancellationToken,
}
impl ScopedPeerStreams {
    pub fn cancellation(&self) -> CancellationToken {
        self.cancel.clone()
    }
}
impl Drop for ScopedPeerStreams {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
impl zero_core::InboundTransportMultiplexer for ScopedPeerStreams {
    type Stream = MkcpStream;
    async fn accept_stream(&self) -> Option<MkcpStream> {
        self.streams.lock().await.recv().await
    }
    fn close(&self) {
        self.cancel.cancel();
    }
}
impl ListenerProfile {
    pub fn accept_peer_scoped(
        &self,
        peer: DatagramPeer,
        masks: &[Mask],
        caller: &AcceptedCaller,
        cancel: CancellationToken,
    ) -> Result<ScopedPeerStreams, ChannelError> {
        if caller.origin()? != AcceptedOrigin::Datagram {
            return Err(ChannelError::new(
                Stage::Prepare,
                Reason::StalePlan,
                Impact::Session,
            ));
        }
        let facts = peer.connection_facts().map_err(failure)?;
        let local = facts
            .local_endpoint
            .ok_or_else(|| failure(io::Error::other("missing native peer endpoint")))?;
        let remote = facts
            .original_source
            .ok_or_else(|| failure(io::Error::other("missing native peer source")))?;
        let codec = Arc::new(std::sync::Mutex::new(
            Codec::new(masks, true).map_err(failure)?,
        ));
        let (tx, rx, _) = peer.split();
        // This view only sends through the original host grant. It never opens
        // a UDP socket, and cannot be mistaken for another native close provider.
        let socket = PacketSocket::Carrier(Arc::new(Socket {
            tx,
            local,
            codec: codec.clone(),
        }));
        let (opened, streams) = mpsc::channel(16);
        let stopping = cancel.child_token();
        let cancel = stopping.clone();
        let profile = self.clone();
        caller.spawn(TaskClass::Auxiliary, move |_| async move {
            let _stop_on_exit = stopping.clone().drop_guard();
            tokio::select! { biased;
                _=stopping.cancelled()=>Ok(()),
                result=profile.drive_peer(socket, remote, Packets::Native(rx), codec, opened)=>result,
            }
        })?;
        Ok(ScopedPeerStreams {
            streams: Mutex::new(streams),
            cancel,
        })
    }
    #[cfg(feature = "quic")]
    pub fn accept_carrier_peer_scoped(
        &self,
        peer: crate::finalmask::owned::source::Peer,
        caller: &AcceptedCaller,
        cancel: CancellationToken,
    ) -> Result<ScopedPeerStreams, ChannelError> {
        if caller.origin()? != AcceptedOrigin::Carrier {
            return Err(ChannelError::new(
                Stage::Prepare,
                Reason::StalePlan,
                Impact::Session,
            ));
        }
        let crate::finalmask::owned::source::Peer {
            socket,
            remote,
            packets,
            ..
        } = peer;
        let codec = Arc::new(std::sync::Mutex::new(
            Codec::new(&[], true).map_err(failure)?,
        ));
        let (opened, streams) = mpsc::channel(16);
        let stopping = cancel.child_token();
        let cancel = stopping.clone();
        let profile = self.clone();
        caller.spawn(TaskClass::Auxiliary,move |_|async move {
            let _stop_on_exit=stopping.clone().drop_guard();
            tokio::select! {biased;
                _=stopping.cancelled()=>Ok(()),
                result=profile.drive_peer(socket,remote,Packets::Carrier(packets),codec,opened)=>result,
            }
        })?;
        Ok(ScopedPeerStreams {
            streams: Mutex::new(streams),
            cancel,
        })
    }
    async fn drive_peer(
        self,
        socket: PacketSocket,
        remote: std::net::SocketAddr,
        mut rx: Packets,
        codec: Arc<std::sync::Mutex<Codec>>,
        opened: mpsc::Sender<MkcpStream>,
    ) -> Result<(), ChannelError> {
        let mut conversations = HashMap::<u16, mpsc::Sender<Vec<u8>>>::new();
        let mut jobs = FuturesUnordered::<BoxFuture<'static, (u16, io::Result<()>)>>::new();
        loop {
            tokio::select! { biased;
                _=opened.closed()=>return Ok(()),
                Some((conv,result))=jobs.next(),if !jobs.is_empty()=>{
                    conversations.remove(&conv);
                    result.map_err(failure)?;
                    if conversations.is_empty() { return Ok(()); }
                }
                packet=rx.recv()=>{
                    let Some(packet)=packet? else { return Ok(()); };
                    let Ok(packet)=codec.lock().unwrap_or_else(|e|e.into_inner()).decode(&packet) else {continue;};
                    let Ok(first)=Segment::decode(&mut packet.as_slice()) else {continue;};
                    if let std::collections::hash_map::Entry::Vacant(entry)=conversations.entry(first.conv) {
                        if matches!(first.body,Body::Command{command:TERMINATE,..}) {continue;}
                        let Ok(permit)=self.admission.clone().try_acquire_owned() else {continue;};
                        let (tx,rx)=mpsc::channel(128);
                        let (stream,driver)=connection(socket.clone(),remote,first.conv,self.settings,Input::Packets(rx)).map_err(failure)?;
                        if opened.try_send(stream).is_err() {continue;}
                        let conv=first.conv;
                        jobs.push(Box::pin(async move {let _permit=permit;(conv,driver.await)}));
                        entry.insert(tx);
                    }
                    let _=conversations[&first.conv].try_send(packet);
                }
            }
        }
    }
}
struct Socket {
    tx: PeerTx,
    local: std::net::SocketAddr,
    codec: Arc<std::sync::Mutex<Codec>>,
}
impl PacketSocketIo for Socket {
    type Error = io::Error;
    fn local_addr(&self) -> io::Result<SocketAddress> {
        Ok(zero_platform_tokio::socket_addr_to_socket_address(
            self.local,
        ))
    }
    fn poll_recv_from(
        &self,
        _: &mut Context<'_>,
        _: &mut [u8],
    ) -> Poll<io::Result<(usize, SocketAddress)>> {
        Poll::Ready(Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "the original peer demultiplexer owns receive",
        )))
    }
    fn send_to<'a>(
        &'a self,
        bytes: &'a [u8],
        peer: SocketAddress,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<usize>> + Send + 'a>> {
        Box::pin(async move {
            let peer = zero_platform_tokio::socket_address_to_socket_addr(peer);
            let emissions = self
                .codec
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .encode_for(peer, bytes)?;
            for emission in emissions {
                if !emission.delay.is_zero() {
                    tokio::time::sleep(emission.delay).await;
                }
                self.tx.send_to(&emission.packet, peer).await?;
            }
            Ok(bytes.len())
        })
    }
}
fn failure(source: impl std::error::Error + Send + Sync + 'static) -> ChannelError {
    ChannelError::new(Stage::Receive, Reason::Io, Impact::Session).with_source(source)
}

enum Packets {
    Native(PeerRx),
    #[cfg(feature = "quic")]
    Carrier(mpsc::Receiver<Vec<u8>>),
}
impl Packets {
    async fn recv(&mut self) -> Result<Option<Vec<u8>>, ChannelError> {
        match self {
            Self::Native(rx) => std::future::poll_fn(|cx| rx.poll_recv(cx)).await,
            #[cfg(feature = "quic")]
            Self::Carrier(rx) => Ok(tokio::time::timeout(
                std::time::Duration::from_secs(300),
                rx.recv(),
            )
            .await
            .unwrap_or(None)),
        }
    }
}

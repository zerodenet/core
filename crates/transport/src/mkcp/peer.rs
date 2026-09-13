use super::{
    config::Settings,
    stream::{connection, Input, MkcpStream},
    wire::{Body, Segment, TERMINATE},
};
use std::{collections::HashMap, io, net::SocketAddr, sync::Arc};
use tokio::{
    sync::{mpsc, Mutex, Semaphore},
    task::JoinHandle,
};

/// One prepared listener shares admission across all remote UDP addresses.
#[derive(Clone)]
pub struct ListenerProfile {
    settings: Settings,
    admission: Arc<Semaphore>,
}
impl ListenerProfile {
    pub fn new(settings: Settings) -> io::Result<Self> {
        Self::new_with_masks(settings, &[])
    }
    pub fn new_with_masks(
        settings: Settings,
        masks: &[crate::finalmask::udp::Mask],
    ) -> io::Result<Self> {
        settings.validate()?;
        crate::finalmask::udp::validate(masks)?;
        Ok(Self {
            settings,
            admission: Arc::new(Semaphore::new(128)),
        })
    }
    pub fn accept_peer(
        &self,
        socket: zero_platform_tokio::PacketSocket,
        peer: SocketAddr,
        packets: mpsc::Receiver<Vec<u8>>,
    ) -> PeerStreams {
        let (opened, streams) = mpsc::channel(16);
        let profile = self.clone();
        let task = tokio::spawn(async move {
            profile.run_peer(socket, peer, packets, opened).await;
        });
        PeerStreams {
            streams: Mutex::new(streams),
            task,
        }
    }
    async fn run_peer(
        self,
        socket: zero_platform_tokio::PacketSocket,
        peer: SocketAddr,
        mut packets: mpsc::Receiver<Vec<u8>>,
        opened: mpsc::Sender<MkcpStream>,
    ) {
        let mut conversations: HashMap<u16, mpsc::Sender<Vec<u8>>> = HashMap::new();
        let mut tasks = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                _=opened.closed()=>break,
                packet=packets.recv()=>{
                    let Some(packet)=packet else {break;};
                    let Ok(first)=Segment::decode(&mut packet.as_slice()) else {continue;};
                    conversations.retain(|_,sender|!sender.is_closed());
                    if let std::collections::hash_map::Entry::Vacant(entry)=conversations.entry(first.conv) {
                        if matches!(first.body,Body::Command{command:TERMINATE,..}) {continue;}
                        let Ok(permit)=self.admission.clone().try_acquire_owned() else {continue;};
                        let (sender,receiver)=mpsc::channel(128);
                        let Ok((stream,driver))=connection(socket.clone(),peer,first.conv,self.settings,Input::Packets(receiver)) else {continue;};
                        if opened.try_send(stream).is_err() {continue;}
                        tasks.spawn(async move {let _permit=permit;let _=driver.await;});
                        entry.insert(sender);
                    }
                    let _=conversations[&first.conv].try_send(packet);
                }
                Some(_)=tasks.join_next()=>{
                    conversations.retain(|_,sender|!sender.is_closed());
                    if conversations.is_empty() {break;}
                }
            }
        }
        tasks.shutdown().await;
    }
}
pub struct PeerStreams {
    streams: Mutex<mpsc::Receiver<MkcpStream>>,
    task: JoinHandle<()>,
}
impl Drop for PeerStreams {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl zero_core::InboundTransportMultiplexer for PeerStreams {
    type Stream = MkcpStream;
    async fn accept_stream(&self) -> Option<Self::Stream> {
        self.streams.lock().await.recv().await
    }
    fn close(&self) {
        self.task.abort();
    }
}

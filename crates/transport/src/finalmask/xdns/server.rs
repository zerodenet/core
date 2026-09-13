use super::*;
use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
};
use tokio::time::Instant;
struct Client {
    last: Instant,
    packets: VecDeque<Vec<u8>>,
}
struct Pending {
    message: wire::Message,
    peer: SocketAddr,
    client: SocketAddr,
}
struct Server {
    clients: HashMap<SocketAddr, Client>,
    pending: Option<Pending>,
    queued_bytes: usize,
}
impl Server {
    fn client(&mut self, peer: SocketAddr) -> Option<&mut Client> {
        if !self.clients.contains_key(&peer) && self.clients.len() >= 4096 {
            return None;
        }
        let client = self.clients.entry(peer).or_insert_with(|| Client {
            last: Instant::now(),
            packets: VecDeque::new(),
        });
        client.last = Instant::now();
        Some(client)
    }
    fn response(&mut self) -> io::Result<Option<Packet>> {
        let Some(record) = self.pending.take() else {
            return Ok(None);
        };
        let mut payload = Vec::new();
        let mut length = 0;
        if let Some(client) = self.clients.get_mut(&record.client) {
            while let Some(packet) = client.packets.front() {
                if length + packet.len() + 2 > codec::MAX_PAYLOAD {
                    break;
                }
                let packet = client.packets.pop_front().unwrap();
                length += packet.len() + 2;
                self.queued_bytes -= packet.len();
                payload.push(packet);
            }
        }
        Ok(Some(Packet {
            bytes: codec::response(record.message, &payload)?,
            peer: record.peer,
        }))
    }
    async fn flush(&mut self, inner: &Arc<dyn AsyncUdpSocket>) -> io::Result<()> {
        if let Some(packet) = self.response()? {
            queue::send(inner, packet).await?;
        }
        Ok(())
    }
}
pub(super) async fn run(
    inner: Arc<dyn AsyncUdpSocket>,
    mut channels: Channels,
    domain: wire::Name,
) -> io::Result<()> {
    let mut state = Server {
        clients: HashMap::new(),
        pending: None,
        queued_bytes: 0,
    };
    let deadline = tokio::time::sleep(Duration::from_secs(1));
    tokio::pin!(deadline);
    let mut cleanup = tokio::time::interval(Duration::from_secs(5));
    cleanup.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _=channels.incoming.closed()=>return Ok(()),
            packet=channels.outgoing.recv()=>{
                let Some(packet)=packet else {return Ok(());};
                if state.queued_bytes+packet.bytes.len()>16*1024*1024 {continue;}
                let Some(client)=state.client(packet.peer) else {continue;};
                if client.packets.len()>=512 {continue;}
                let n=packet.bytes.len();client.packets.push_back(packet.bytes);state.queued_bytes+=n;
                if state.pending.as_ref().is_some_and(|pending|pending.client==packet.peer) {state.flush(&inner).await?;}
            }
            received=queue::receive(inner.as_ref())=>{
                for packet in received? {
                    let Ok(message)=wire::parse(&packet.bytes) else {continue;};
                    let Some((response,id,packets))=codec::response_for(message,&domain) else {continue;};
                    state.flush(&inner).await?;
                    let Some(id)=id else {queue::send(&inner,Packet {bytes:codec::response(response,&[])?,peer:packet.peer}).await?;continue;};
                    let peer=codec::client_address(id);
                    let Some(client)=state.client(peer) else {continue;};
                    let ready=!client.packets.is_empty();
                    for bytes in packets {let _=channels.incoming.try_send(Packet {bytes,peer});}
                    state.pending=Some(Pending {message:response,peer:packet.peer,client:peer});
                    if ready {state.flush(&inner).await?;} else {deadline.as_mut().reset(Instant::now()+Duration::from_secs(1));}
                }
            }
            _=&mut deadline,if state.pending.is_some()=>state.flush(&inner).await?,
            _=cleanup.tick()=>{
                state.clients.retain(|_,client| {
                    if client.last.elapsed()<Duration::from_secs(10) {true} else {state.queued_bytes-=client.packets.iter().map(Vec::len).sum::<usize>();false}
                });
            }
        }
    }
}

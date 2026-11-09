use super::super::{fifo::Fifo, memory::Memory, rows::Rows};
use super::*;
use std::net::SocketAddr;
use tokio::time::Instant;
struct Client {
    last: Instant,
    packets: Fifo,
}
struct Pending {
    message: wire::Message,
    peer: SocketAddr,
    client: SocketAddr,
    _original: Packet,
}
struct Server {
    clients: Rows<SocketAddr, Client>,
    pending: Option<Pending>,
    queued_bytes: usize,
    memory: Memory,
}
impl Server {
    fn client(&mut self, peer: SocketAddr) -> Option<&mut Client> {
        if self.clients.get(&peer).is_none() {
            self.clients.reserve(&peer).ok()?;
            self.clients.insert_reserved(
                peer,
                Client {
                    last: Instant::now(),
                    packets: Fifo::new(self.memory.clone()),
                },
            );
        }
        let client = self.clients.get_mut(&peer)?;
        client.last = Instant::now();
        Some(client)
    }
    fn response(&mut self) -> io::Result<Option<Packet>> {
        let Some(record) = self.pending.take() else {
            return Ok(None);
        };
        let credit = self.memory.control(4 * 65536 + 64)?;
        let mut payload = Vec::new();
        let mut length = 0;
        if let Some(client) = self.clients.get_mut(&record.client) {
            while let Some(packet) = client.packets.front() {
                if length + packet.bytes.len() + 2 > codec::MAX_PAYLOAD {
                    break;
                }
                let packet = client.packets.pop().unwrap();
                length += packet.bytes.len() + 2;
                self.queued_bytes -= packet.bytes.len();
                payload.push(packet);
            }
        }
        let bytes = codec::response_frames(record.message, &payload)?;
        Ok(Some(Packet {
            _credit: credit.map(Into::into),
            bytes,
            peer: record.peer,
            metadata: None,
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
    let memory = channels.mask_memory.clone();
    let mut scratch = memory.scratch()?;
    let mut clients = Rows::new(4096);
    clients.install(memory.clone());
    let mut state = Server {
        clients,
        pending: None,
        queued_bytes: 0,
        memory: memory.clone(),
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
                let n=packet.bytes.len();let original_peer=packet.peer;
                if client.packets.push(packet).is_err() { continue; } state.queued_bytes+=n;
                if state.pending.as_ref().is_some_and(|pending|pending.client==original_peer) {state.flush(&inner).await?;}
            }
            received=super::receive::receive(inner.as_ref(), &mut scratch, &memory)=>{
                for mut packet in received? {
                    if let Some(metadata) = memory.metadata() {
                        if packet.share_credit(metadata).is_err() { continue; }
                    }
                    let Ok(message)=wire::parse(&packet.bytes) else {continue;};
                    let Some((response,id,packets))=codec::response_for(message,&domain) else {continue;};
                    state.flush(&inner).await?;
                    let Some(id)=id else {let credit=memory.control(4*65536+64)?;queue::send(&inner,Packet { _credit: credit.map(Into::into),bytes:codec::response(response,&[])?,peer:packet.peer,metadata:None}).await?;continue;};
                    let peer=codec::client_address(id);
                    let Some(client)=state.client(peer) else {continue;};
                    let ready=!client.packets.is_empty();
                    for bytes in packets {let _=channels.incoming.try_send(Packet { bytes, peer, metadata: None, _credit: packet._credit.as_ref().map(|credit|credit.shared()) });}
                    state.pending=Some(Pending {message:response,peer:packet.peer,client:peer,_original:packet});
                    if ready {state.flush(&inner).await?;} else {deadline.as_mut().reset(Instant::now()+Duration::from_secs(1));}
                }
            }
            _=&mut deadline,if state.pending.is_some()=>state.flush(&inner).await?,
            _=cleanup.tick()=>{
                state.clients.retain(|_,client| {
                    if client.last.elapsed()<Duration::from_secs(10) {true} else {state.queued_bytes-=client.packets.bytes();false}
                });
            }
        }
    }
}

use super::*;
use std::collections::{HashMap, VecDeque};
use tokio::time::Instant;
struct Client {
    last: Instant,
    packets: VecDeque<Vec<u8>>,
}
struct Pending {
    peer: SocketAddr,
    sequence: u16,
    first: Option<u8>,
}
struct State {
    clients: HashMap<SocketAddr, Client>,
    pending: Option<Pending>,
    bytes: usize,
}
impl State {
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
    async fn flush(&mut self, raw: &IcmpSocket, ipv6: bool) -> io::Result<()> {
        let Some(pending) = self.pending.take() else {
            return Ok(());
        };
        let Some(packet) = self
            .clients
            .get_mut(&pending.peer)
            .and_then(|client| client.packets.pop_front())
        else {
            return Ok(());
        };
        self.bytes -= packet.len();
        let wire = codec::reply(
            ipv6,
            pending.peer.port(),
            pending.sequence,
            pending.first,
            &packet,
        )?;
        raw.send_to(&wire, pending.peer.ip()).await?;
        Ok(())
    }
}
pub(super) async fn run(
    raw: Arc<IcmpSocket>,
    mut channels: Channels,
    id: u16,
    ipv6: bool,
) -> io::Result<()> {
    let mut state = State {
        clients: HashMap::new(),
        pending: None,
        bytes: 0,
    };
    let mut wire = vec![0u8; 65536];
    let deadline = tokio::time::sleep(Duration::from_secs(1));
    tokio::pin!(deadline);
    let mut cleanup = tokio::time::interval(Duration::from_secs(5));
    cleanup.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _=channels.incoming.closed()=>return Ok(()),
            packet=channels.outgoing.recv()=>{
                let Some(packet)=packet else {return Ok(());};
                if state.bytes+packet.bytes.len()>16*1024*1024 {continue;}
                let Some(client)=state.client(packet.peer) else {continue;};
                if client.packets.len()>=512 {continue;}
                let n=packet.bytes.len();client.packets.push_back(packet.bytes);state.bytes+=n;
                if state.pending.as_ref().is_some_and(|p|p.peer==packet.peer) {state.flush(&raw,ipv6).await?;}
            }
            received=raw.recv_from(&mut wire)=>{
                let(n,source)=received?;let Ok(echo)=codec::parse(&wire[..n],ipv6,false) else {continue;};
                if id!=0 && id!=echo.id {continue;}
                let peer=SocketAddr::new(source,echo.id);
                state.flush(&raw,ipv6).await?;
                let Some(client)=state.client(peer) else {continue;};let ready=!client.packets.is_empty();
                if !echo.payload.is_empty() {let _=channels.incoming.try_send(Packet {bytes:echo.payload.to_vec(),peer});}
                state.pending=Some(Pending {peer,sequence:echo.sequence,first:echo.payload.first().copied()});
                if ready {state.flush(&raw,ipv6).await?;} else {deadline.as_mut().reset(Instant::now()+Duration::from_secs(1));}
            }
            _=&mut deadline,if state.pending.is_some()=>state.flush(&raw,ipv6).await?,
            _=cleanup.tick()=>state.clients.retain(|_,client|if client.last.elapsed()<Duration::from_secs(10){true}else{state.bytes-=client.packets.iter().map(Vec::len).sum::<usize>();false}),
        }
    }
}

use super::super::{fifo::Fifo, memory::Memory, rows::Rows};
use super::*;
use tokio::time::Instant;
struct Client {
    last: Instant,
    packets: Fifo,
}
struct Pending {
    peer: SocketAddr,
    sequence: u16,
    first: Option<u8>,
}
struct State {
    clients: Rows<SocketAddr, Client>,
    pending: Option<Pending>,
    bytes: usize,
    memory: Memory,
}
impl State {
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
    async fn flush(&mut self, raw: &mut native::Socket, ipv6: bool) -> io::Result<()> {
        let Some(pending) = self.pending.take() else {
            return Ok(());
        };
        let Some(packet) = self
            .clients
            .get_mut(&pending.peer)
            .and_then(|client| client.packets.pop())
        else {
            return Ok(());
        };
        self.bytes -= packet.bytes.len();
        let _working = self.memory.control(4 * 65536 + 64)?;
        let wire = codec::reply(
            ipv6,
            pending.peer.port(),
            pending.sequence,
            pending.first,
            &packet.bytes,
        )?;
        raw.send_to(&wire, pending.peer.ip()).await?;
        Ok(())
    }
}
pub(super) async fn run(
    mut raw: native::Socket,
    mut channels: Channels,
    id: u16,
    ipv6: bool,
) -> io::Result<()> {
    let memory = channels.mask_memory.clone();
    let mut scratch = memory.scratch()?;
    let mut clients = Rows::new(4096);
    clients.install(memory.clone());
    let mut state = State {
        clients,
        pending: None,
        bytes: 0,
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
                if state.bytes+packet.bytes.len()>16*1024*1024 {continue;}
                let Some(client)=state.client(packet.peer) else {continue;};
                if client.packets.len()>=512 {continue;}
                let n=packet.bytes.len();let original_peer=packet.peer;
                if client.packets.push(packet).is_err() { continue; } state.bytes+=n;
                if state.pending.as_ref().is_some_and(|p|p.peer==original_peer) {state.flush(&mut raw,ipv6).await?;}
            }
            received=raw.recv_from(&mut scratch.bytes)=>{
                let(n,source)=received?;let Ok(echo)=codec::parse(&scratch.bytes[..n],ipv6,false) else {continue;};
                if id!=0 && id!=echo.id {continue;}
                let peer=SocketAddr::new(source,echo.id);
                state.flush(&mut raw,ipv6).await?;
                let Some(client)=state.client(peer) else {continue;};let ready=!client.packets.is_empty();
                if !echo.payload.is_empty() {if let Ok(packet)=memory.copy(echo.payload,peer) {let _=channels.incoming.try_send(packet);}}
                state.pending=Some(Pending {peer,sequence:echo.sequence,first:echo.payload.first().copied()});
                if ready {state.flush(&mut raw,ipv6).await?;} else {deadline.as_mut().reset(Instant::now()+Duration::from_secs(1));}
            }
            _=&mut deadline,if state.pending.is_some()=>state.flush(&mut raw,ipv6).await?,
            _=cleanup.tick()=>state.clients.retain(|_,client|if client.last.elapsed()<Duration::from_secs(10){true}else{state.bytes-=client.packets.bytes();false}),
        }
    }
}

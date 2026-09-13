//! Xray XDNS tunneling: DNS TXT requests, logical client IDs and polling.
use super::queue::{self, Channels, Packet};
use quinn::AsyncUdpSocket;
use std::{io, sync::Arc, time::Duration};
mod codec;
mod server;
mod wire;
pub(super) fn validate(domain: &str) -> io::Result<()> {
    wire::domain(domain).map(|_| ())
}
pub(super) fn wrap(
    inner: Arc<dyn AsyncUdpSocket>,
    domain: &str,
    server: bool,
) -> io::Result<Arc<dyn AsyncUdpSocket>> {
    let domain = wire::domain(domain)?;
    if server {
        let prepare = Arc::new(|transmit: &quinn::udp::Transmit<'_>| {
            if transmit.contents.len() + 2 > codec::MAX_PAYLOAD {
                return Err(wire::invalid());
            }
            Ok(transmit.contents.to_vec())
        });
        return queue::spawn(inner, prepare, move |inner, channels| {
            server::run(inner, channels, domain)
        });
    }
    let id = rand::random::<[u8; 8]>();
    let encoded_domain = domain.clone();
    let prepare = Arc::new(move |transmit: &quinn::udp::Transmit<'_>| {
        codec::query(&encoded_domain, &id, transmit.contents)
    });
    queue::spawn(inner, prepare, move |inner, channels| {
        client(inner, channels, domain, id)
    })
}
async fn client(
    inner: Arc<dyn AsyncUdpSocket>,
    mut channels: Channels,
    domain: wire::Name,
    id: [u8; 8],
) -> io::Result<()> {
    let mut peer = None;
    let mut delay = Duration::from_millis(500);
    let timer = tokio::time::sleep(delay);
    tokio::pin!(timer);
    loop {
        tokio::select! {
            biased;
            _=channels.incoming.closed()=>return Ok(()),
            outgoing=channels.outgoing.recv()=>{
                let Some(packet)=outgoing else {return Ok(());};
                peer=Some(packet.peer);queue::send(&inner,packet).await?;
                delay=Duration::from_millis(500);timer.as_mut().reset(tokio::time::Instant::now()+delay);
            }
            incoming=queue::receive(inner.as_ref())=>{
                let mut poll=false;
                for packet in incoming? {
                    let Ok(message)=wire::parse(&packet.bytes) else {continue;};
                    let Ok(packets)=codec::response_packets(message,&domain) else {continue;};
                    poll|=!packets.is_empty();
                    for bytes in packets {let _=channels.incoming.try_send(Packet {bytes,peer:packet.peer});}
                }
                if poll { if let Some(peer)=peer {queue::send(&inner,Packet {bytes:codec::query(&domain,&id,&[])?,peer}).await?;} delay=Duration::from_millis(500);timer.as_mut().reset(tokio::time::Instant::now()+delay); }
            }
            _=&mut timer=>{
                if let Some(peer)=peer {queue::send(&inner,Packet {bytes:codec::query(&domain,&id,&[])?,peer}).await?;}
                delay=(delay*2).min(Duration::from_secs(10));timer.as_mut().reset(tokio::time::Instant::now()+delay);
            }
        }
    }
}
#[cfg(test)]
#[path = "../../tests/finalmask/xdns.rs"]
mod tests;

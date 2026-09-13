//! ICMP echo tunneling. Raw-socket operations stay in the platform crate.
use super::queue::{self, Channels, Packet};
use quinn::AsyncUdpSocket;
use std::{
    io,
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex},
    time::Duration,
};
use zero_platform_tokio::{EgressInterface, IcmpSocket};
mod codec;
mod server;
pub(super) fn address(ip: &str) -> io::Result<IpAddr> {
    if ip.is_empty() {
        return Ok(std::net::Ipv4Addr::UNSPECIFIED.into());
    }
    ip.parse()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid XICMP bind IP"))
}
pub(super) fn wrap(
    inner: Arc<dyn AsyncUdpSocket>,
    ip: &str,
    id: u16,
    server: bool,
    egress: Option<&EgressInterface>,
) -> io::Result<Arc<dyn AsyncUdpSocket>> {
    let ip = address(ip)?;
    let ipv6 = ip.is_ipv6();
    let raw = Arc::new(IcmpSocket::bind(ip, egress)?);
    let id = if id == 0 && !server {
        rand::random()
    } else {
        id
    };
    let local = SocketAddr::new(raw.local_addr()?, if server { 0 } else { id });
    if server {
        let prepare = Arc::new(|t: &quinn::udp::Transmit<'_>| {
            if t.contents.len() + 9 > 65535 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "XICMP packet too large",
                ));
            }
            Ok(t.contents.to_vec())
        });
        return queue::spawn_at(
            inner,
            local,
            prepare,
            move |keepalive, channels| async move {
                let _keepalive = keepalive;
                server::run(raw, channels, id, ipv6).await
            },
        );
    }
    let state = Arc::new(Mutex::new(codec::Client::new(id, ipv6)));
    let encoder = state.clone();
    let prepare = Arc::new(move |t: &quinn::udp::Transmit<'_>| {
        encoder.lock().unwrap().query(t.contents, t.destination)
    });
    queue::spawn_at(
        inner,
        local,
        prepare,
        move |keepalive, channels| async move {
            let _keepalive = keepalive;
            client(raw, channels, state).await
        },
    )
}
async fn client(
    raw: Arc<IcmpSocket>,
    mut channels: Channels,
    state: Arc<Mutex<codec::Client>>,
) -> io::Result<()> {
    let mut destination = None;
    let mut delay = Duration::from_millis(500);
    let timer = tokio::time::sleep(delay);
    tokio::pin!(timer);
    let mut wire = vec![0u8; 65536];
    loop {
        tokio::select! {
            biased;
            _=channels.incoming.closed()=>return Ok(()),
            packet=channels.outgoing.recv()=>{
                let Some(packet)=packet else {return Ok(());};
                destination=Some(packet.peer);raw.send_to(&packet.bytes,packet.peer.ip()).await?;
                delay=Duration::from_millis(500);timer.as_mut().reset(tokio::time::Instant::now()+delay);
            }
            received=raw.recv_from(&mut wire)=>{
                let (n,source)=received?;
                let decoded=state.lock().unwrap().response(&wire[..n],source);
                if let Ok(Some((bytes,peer)))=decoded {
                    let _=channels.incoming.try_send(Packet {bytes,peer});
                    if let Some(peer)=destination {let bytes=state.lock().unwrap().query(&[],peer)?;raw.send_to(&bytes,peer.ip()).await?;}
                    delay=Duration::from_millis(500);timer.as_mut().reset(tokio::time::Instant::now()+delay);
                }
            }
            _=&mut timer=>{
                if let Some(peer)=destination {let bytes=state.lock().unwrap().query(&[],peer)?;raw.send_to(&bytes,peer.ip()).await?;}
                delay=(delay*2).min(Duration::from_secs(10));timer.as_mut().reset(tokio::time::Instant::now()+delay);
            }
        }
    }
}
#[cfg(test)]
#[path = "../../tests/finalmask/xicmp.rs"]
mod tests;

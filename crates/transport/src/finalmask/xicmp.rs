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
#[path = "xicmp/io.rs"]
mod native;
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
                server::run(native::Socket::Legacy(raw), channels, id, ipv6).await
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
            client(native::Socket::Legacy(raw), channels, state).await
        },
    )
}
pub(super) fn prepare_owned(
    inner: Arc<dyn AsyncUdpSocket>,
    raw: zero_platform_tokio::controlled_icmp::ControlledIcmpSocket,
    id: u16,
    server: bool,
    memory: super::memory::Memory,
) -> io::Result<(
    Arc<dyn AsyncUdpSocket>,
    crate::datagram_driver::PreparedIoDriver,
)> {
    let ip = raw.local_addr()?;
    let ipv6 = ip.is_ipv6();
    if !server {
        let id = if id == 0 { rand::random() } else { id };
        let mut codec = codec::Client::new(id, ipv6);
        codec.install_memory(memory.clone());
        let state = Arc::new(Mutex::new(codec));
        let encoder = state.clone();
        let prepare = Arc::new(move |t: &quinn::udp::Transmit<'_>| {
            encoder.lock().unwrap().query(t.contents, t.destination)
        });
        let (socket, driver) = queue::prepare_mask_with(
            SocketAddr::new(ip, id),
            prepare,
            memory,
            move |channels| async move {
                let _keepalive = inner;
                client(native::Socket::Hosted(raw), channels, state).await
            },
        )?;
        return Ok((socket, driver));
    }
    let prepare = Arc::new(|t: &quinn::udp::Transmit<'_>| {
        if t.contents.len() + 9 > 65535 {
            return Err(io::Error::other("XICMP packet too large"));
        }
        Ok(t.contents.to_vec())
    });
    let (socket, driver) = queue::prepare_mask_with(
        SocketAddr::new(ip, 0),
        prepare,
        memory,
        move |channels| async move {
            let _keepalive = inner;
            server::run(native::Socket::Hosted(raw), channels, id, ipv6).await
        },
    )?;
    Ok((socket, driver))
}
async fn client(
    mut raw: native::Socket,
    mut channels: Channels,
    state: Arc<Mutex<codec::Client>>,
) -> io::Result<()> {
    let mut destination = None;
    let mut delay = Duration::from_millis(500);
    let timer = tokio::time::sleep(delay);
    tokio::pin!(timer);
    let memory = channels.mask_memory.clone();
    let mut scratch = memory.scratch()?;
    loop {
        tokio::select! {
            biased;
            _=channels.incoming.closed()=>return Ok(()),
            packet=channels.outgoing.recv()=>{
                let Some(packet)=packet else {return Ok(());};
                destination=Some(packet.peer);raw.send_to(&packet.bytes,packet.peer.ip()).await?;
                delay=Duration::from_millis(500);timer.as_mut().reset(tokio::time::Instant::now()+delay);
            }
            received=raw.recv_from(&mut scratch.bytes)=>{
                let (n,source)=received?;
                let working = memory.receive(3*65536)?;
                let decoded=state.lock().unwrap().response(&scratch.bytes[..n],source);
                if let Ok(Some((bytes,peer)))=decoded {
                    let _=channels.incoming.try_send(Packet { bytes, peer, metadata: None, _credit: working.map(Into::into) });
                    if let Some(peer)=destination {
                        match poll_query(&state,&memory,peer) {
                            Ok(packet)=>{raw.send_to(&packet.bytes,peer.ip()).await?;}
                            Err(error) if error.kind()==io::ErrorKind::WouldBlock=>{},
                            Err(error)=>return Err(error),
                        }
                    }
                    delay=Duration::from_millis(500);timer.as_mut().reset(tokio::time::Instant::now()+delay);
                }
            }
            _=&mut timer=>{
                if let Some(peer)=destination {
                        match poll_query(&state,&memory,peer) {
                            Ok(packet)=>{raw.send_to(&packet.bytes,peer.ip()).await?;}
                            Err(error) if error.kind()==io::ErrorKind::WouldBlock=>{},
                            Err(error)=>return Err(error),
                        }
                    }
                delay=(delay*2).min(Duration::from_secs(10));timer.as_mut().reset(tokio::time::Instant::now()+delay);
            }
        }
    }
}
fn poll_query(
    state: &Mutex<codec::Client>,
    memory: &super::memory::Memory,
    peer: SocketAddr,
) -> io::Result<Packet> {
    let credit = memory.control(4 * 65536 + 64)?;
    let bytes = {
        let _notifications = memory.metadata().map(|metadata| metadata.notifications());
        state.lock().unwrap().query(&[], peer)?
    };
    Ok(Packet {
        bytes,
        peer,
        metadata: None,
        _credit: credit.map(Into::into),
    })
}
#[cfg(test)]
#[path = "../../tests/finalmask/xicmp.rs"]
mod tests;

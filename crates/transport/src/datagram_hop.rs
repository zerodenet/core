//! Port rotation beneath packet masks, retaining one previous receive socket.
use crate::datagram_queue::{self, Packet};
use rand::Rng;
use std::{io, net::SocketAddr, sync::Arc, time::Duration};
#[derive(Clone, Debug)]
pub struct Profile {
    ports: Arc<[u16]>,
    min: u64,
    max: u64,
}
#[derive(Clone, Copy, Debug, Default)]
pub struct OptionsRef<'a> {
    pub ports: &'a [u16],
    pub interval_min_secs: u64,
    pub interval_max_secs: u64,
}
impl Profile {
    pub fn new(options: OptionsRef<'_>) -> io::Result<Self> {
        if options.ports.is_empty() || options.ports.len() > 65535 || options.ports.contains(&0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid UDP hopping ports",
            ));
        }
        if [options.interval_min_secs, options.interval_max_secs]
            .iter()
            .any(|value| *value != 0 && *value < 5)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "UDP hop interval must be at least five seconds",
            ));
        }
        let (min, max) = if options.interval_min_secs == 0 || options.interval_max_secs == 0 {
            (30, 30)
        } else {
            (
                options.interval_min_secs.min(options.interval_max_secs),
                options.interval_min_secs.max(options.interval_max_secs),
            )
        };
        if min < 5 || max > i64::MAX as u64 / 1_000_000_000 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "UDP hop interval must be at least five seconds and fit a duration",
            ));
        }
        Ok(Self {
            ports: options.ports.into(),
            min,
            max,
        })
    }
    fn peer(&self, mut peer: SocketAddr) -> SocketAddr {
        peer.set_port(self.ports[rand::rng().random_range(0..self.ports.len())]);
        peer
    }
    fn interval(&self) -> Duration {
        Duration::from_secs(if self.min == self.max {
            self.min
        } else {
            rand::rng().random_range(self.min..self.max)
        })
    }
    pub(crate) fn open(
        &self,
        factory: crate::OutboundDatagramSocketFactory,
        logical: SocketAddr,
    ) -> io::Result<Arc<dyn quinn::AsyncUdpSocket>> {
        if factory.is_relay() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "UDP hopping requires the outermost datagram carrier",
            ));
        }
        let peer = self.peer(logical);
        let socket = native(&factory, peer)?;
        let local = socket.local_addr()?;
        let profile = self.clone();
        datagram_queue::spawn_with(
            local,
            Arc::new(move |packet| {
                if packet.destination != logical {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "UDP hopping changed server",
                    ));
                }
                Ok(packet.contents.to_vec())
            }),
            move |mut channels| async move {
                let mut current = socket;
                let mut current_peer = peer;
                let mut previous: Option<(Arc<dyn quinn::AsyncUdpSocket>, SocketAddr)> = None;
                let timer = tokio::time::sleep(profile.interval());
                tokio::pin!(timer);
                loop {
                    tokio::select! {
                        packet = channels.outgoing.recv() => {
                            let Some(mut packet) = packet else { return Ok(()); };
                            packet.peer = current_peer;
                            datagram_queue::send(&current,packet).await?;
                        }
                        packets = datagram_queue::receive(current.as_ref()) => {
                            for packet in packets? { forward(&channels.incoming,packet,current_peer,logical); }
                        }
                        packets = async { datagram_queue::receive(previous.as_ref().unwrap().0.as_ref()).await }, if previous.is_some() => {
                            match packets {
                                Ok(packets) => for packet in packets { forward(&channels.incoming,packet,previous.as_ref().unwrap().1,logical); },
                                Err(_) => { previous = None; }
                            }
                        }
                        _ = &mut timer => {
                            let peer = profile.peer(logical);
                            match native(&factory,peer) {
                                Ok(socket) => { previous = Some((std::mem::replace(&mut current,socket),current_peer)); current_peer = peer; }
                                Err(error) => tracing::debug!(%error,"UDP port hop skipped"),
                            }
                            timer.as_mut().reset(tokio::time::Instant::now()+profile.interval());
                        }
                    }
                }
            },
        )
    }
}
fn native(
    factory: &crate::OutboundDatagramSocketFactory,
    peer: SocketAddr,
) -> io::Result<Arc<dyn quinn::AsyncUdpSocket>> {
    quinn::Runtime::wrap_udp_socket(&quinn::TokioRuntime, factory.bind_std(peer)?)
}
fn forward(
    sender: &tokio::sync::mpsc::Sender<Packet>,
    mut packet: Packet,
    expected: SocketAddr,
    logical: SocketAddr,
) {
    if packet.peer == expected {
        packet.peer = logical;
        let _ = sender.try_send(packet);
    }
}
#[cfg(test)]
#[path = "../tests/datagram_hop/mod.rs"]
mod tests;

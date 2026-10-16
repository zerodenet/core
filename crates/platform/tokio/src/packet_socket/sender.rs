//! Per-datagram egress for a shared listener, without constraining reception.
use crate::{EgressInterfaceControl, EgressSelection, PacketSocket};
use std::{
    collections::HashMap,
    io,
    net::SocketAddr,
    time::{Duration, Instant},
};
use tokio::io::Interest;

/// A single listener's send side. The socket and its local port remain shared
/// with the receive side; only the outgoing datagram carries an interface hint.
/// Route hints expire after one second and immediately on topology changes.
/// No protocol framing or endpoint policy belongs here.
///
/// This handle is the socket's sole writer. Other native socket handles may
/// receive, but must not concurrently send or mutate interface socket options:
/// Darwin's specifically bound IPv4 fallback uses a synchronous temporary scope.
pub struct PacketSocketSender {
    socket: PacketSocket,
    control: EgressInterfaceControl,
    selections: HashMap<SocketAddr, (EgressSelection, Instant)>,
}

impl PacketSocketSender {
    pub fn new(socket: PacketSocket, control: EgressInterfaceControl) -> Self {
        Self {
            socket,
            control,
            selections: HashMap::new(),
        }
    }

    pub async fn send_to(&mut self, bytes: &[u8], peer: SocketAddr) -> io::Result<usize> {
        let PacketSocket::Udp(socket) = &self.socket else {
            // An opaque carrier owns its transport's underlay selection.
            return self.socket.send_to(bytes, peer).await;
        };
        loop {
            socket.writable().await?;
            let generation = self.control.generation();
            let now = Instant::now();
            if self.selections.get(&peer).is_none_or(|(selection, until)| {
                selection.generation() != generation || *until <= now
            }) {
                // Bound memory even for authenticated roaming addresses. A full
                // cache evicts hints only; no packets or peer state are dropped.
                if self.selections.len() >= 256 {
                    self.selections.clear();
                }
                self.selections.insert(
                    peer,
                    (
                        self.control.select_for_peer(peer),
                        now + Duration::from_secs(1),
                    ),
                );
            }
            let selection = &self.selections[&peer].0;
            selection.ensure_connectable()?;
            if selection.generation() != self.control.generation() {
                continue;
            }
            let result = socket.try_io(Interest::WRITABLE, || {
                super::ancillary::send(socket, bytes, peer, selection.interface())
            });
            match result {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
                Err(error) => {
                    self.selections.remove(&peer);
                    return Err(error);
                }
                Ok(size) => return Ok(size),
            }
        }
    }
}

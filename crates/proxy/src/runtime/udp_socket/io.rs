//! Direct reply identity follows the socket when client sessions share a target.
use super::*;
use futures_util::{stream::FuturesUnordered, StreamExt};

#[derive(Clone, Copy)]
pub(crate) struct DirectUdpResponseSource {
    pub(crate) sender: SocketAddr,
    pub(crate) session_id: Option<u64>,
}
impl DirectUdpSockets {
    pub(crate) fn isolate_session(&mut self, session_id: u64) {
        self.isolated_sessions.insert(session_id);
    }
    pub(crate) fn retire_session(&mut self, session_id: u64) {
        self.isolated_sessions.remove(&session_id);
        self.sockets
            .retain(|entry| entry.session_id != Some(session_id));
    }
    pub(crate) async fn send_to_addr(
        &mut self,
        services: &crate::protocol_registry::UdpNetworkServices,
        payload: &[u8],
        target: SocketAddr,
        session_id: u64,
    ) -> Result<usize, EngineError> {
        let scope = self
            .isolated_sessions
            .contains(&session_id)
            .then_some(session_id);
        let binding = DirectUdpSocketBinding {
            ipv6: target.is_ipv6(),
            egress: services.direct_datagram_egress(target),
        };
        let socket_index = match self
            .sockets
            .iter()
            .position(|socket| socket.binding == binding && socket.session_id == scope)
        {
            Some(index) => index,
            None => {
                let socket = services
                    .bind_direct_datagram_socket(
                        target,
                        if scope.is_some() {
                            None
                        } else {
                            self.preferred_port
                        },
                    )
                    .await?;
                log_direct_socket(if target.is_ipv6() { "IPv6" } else { "IPv4" }, &socket);
                let mut entry = DirectUdpSocket::new(socket, target.is_ipv6());
                entry.session_id = scope;
                self.sockets.push(entry);
                self.sockets.len() - 1
            }
        };
        send_direct_udp_packet(&self.sockets[socket_index].socket, target, payload).await
    }

    pub(crate) async fn recv_from_addr(
        &self,
        output: &mut [u8],
    ) -> Result<(usize, DirectUdpResponseSource), std::io::Error> {
        let mut receives = FuturesUnordered::new();
        for entry in &self.sockets {
            receives.push(async move {
                let mut buffer = entry.receive_buffer.lock().await;
                let result = entry.socket.recv_from_addr(&mut buffer).await;
                (result, buffer, entry.session_id)
            });
        }
        let (result, buffer, session_id) = receives
            .next()
            .await
            .expect("direct UDP socket set is never empty");
        let (size, sender) = result?;
        let size = size.min(output.len());
        output[..size].copy_from_slice(&buffer[..size]);
        Ok((size, DirectUdpResponseSource { sender, session_id }))
    }
}

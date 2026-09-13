//! Async-packet adapter over the same carrier socket used by QUIC.
use quinn::{AsyncUdpSocket, Runtime};
use std::{
    io::{self, IoSliceMut},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use zero_platform_tokio::{
    socket_addr_to_socket_address, socket_address_to_socket_addr, PacketSocket,
};
use zero_traits::{PacketSocketIo, SocketAddress};
pub fn wrap(
    raw: std::net::UdpSocket,
    masks: &[super::udp::Mask],
    server: bool,
) -> io::Result<PacketSocket> {
    wrap_with_egress(raw, masks, server, None)
}
pub fn wrap_with_egress(
    raw: std::net::UdpSocket,
    masks: &[super::udp::Mask],
    server: bool,
    egress: Option<&zero_platform_tokio::EgressInterface>,
) -> io::Result<PacketSocket> {
    raw.set_nonblocking(true)?;
    let socket = super::Socket::wrap_with_egress(
        quinn::TokioRuntime.wrap_udp_socket(raw)?,
        masks,
        server,
        egress,
    )?;
    Ok(from_socket(socket))
}
pub fn from_socket(socket: Arc<dyn AsyncUdpSocket>) -> PacketSocket {
    PacketSocket::Carrier(Arc::new(PacketIo {
        socket,
        pending: Mutex::new(Default::default()),
    }))
}
struct PacketIo {
    socket: Arc<dyn AsyncUdpSocket>,
    pending: Mutex<std::collections::VecDeque<(Vec<u8>, SocketAddress)>>,
}
impl PacketSocketIo for PacketIo {
    type Error = io::Error;
    fn local_addr(&self) -> io::Result<SocketAddress> {
        self.socket.local_addr().map(socket_addr_to_socket_address)
    }
    fn poll_recv_from(
        &self,
        cx: &mut Context<'_>,
        bytes: &mut [u8],
    ) -> Poll<io::Result<(usize, SocketAddress)>> {
        let mut pending = self.pending.lock().unwrap();
        let mut wire = [0u8; 65536];
        for _ in 0..32 {
            while let Some((packet, peer)) = pending.pop_front() {
                if packet.len() > bytes.len() {
                    continue;
                }
                bytes[..packet.len()].copy_from_slice(&packet);
                return Poll::Ready(Ok((packet.len(), peer)));
            }
            let mut buffers = [IoSliceMut::new(&mut wire)];
            let mut meta = [quinn::udp::RecvMeta::default()];
            let count = std::task::ready!(self.socket.poll_recv(cx, &mut buffers, &mut meta))?;
            if count == 0 || meta[0].stride == 0 || meta[0].len > wire.len() {
                continue;
            }
            pending.extend(
                wire[..meta[0].len]
                    .chunks(meta[0].stride)
                    .map(|p| (p.to_vec(), socket_addr_to_socket_address(meta[0].addr))),
            );
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
    fn send_to<'a>(
        &'a self,
        bytes: &'a [u8],
        peer: SocketAddress,
    ) -> Pin<Box<dyn std::future::Future<Output = io::Result<usize>> + Send + 'a>> {
        Box::pin(async move {
            let transmit = quinn::udp::Transmit {
                contents: bytes,
                destination: socket_address_to_socket_addr(peer),
                ecn: None,
                segment_size: None,
                src_ip: None,
            };
            let mut poller = self.socket.clone().create_io_poller();
            loop {
                match self.socket.try_send(&transmit) {
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        std::future::poll_fn(|cx| poller.as_mut().poll_writable(cx)).await?
                    }
                    result => return result.map(|_| bytes.len()),
                }
            }
        })
    }
}

//! QUIC socket decorator; preserves packet destinations and bounds decode work.
use super::udp::{Codec, Mask};
use quinn::{
    udp::{RecvMeta, Transmit},
    AsyncUdpSocket, UdpPoller,
};
use std::{
    io::{self, IoSliceMut},
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
mod send;
pub struct Socket {
    sender: Option<send::Sender>,
    inner: Arc<dyn AsyncUdpSocket>,
    codec: Mutex<Codec>,
    pending: Mutex<std::collections::VecDeque<(Vec<u8>, RecvMeta)>>,
}
impl std::fmt::Debug for Socket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FinalMaskSocket").finish_non_exhaustive()
    }
}
impl Socket {
    pub fn wrap(
        inner: Arc<dyn AsyncUdpSocket>,
        masks: &[Mask],
        server: bool,
    ) -> io::Result<Arc<dyn AsyncUdpSocket>> {
        Self::wrap_with_egress(inner, masks, server, None)
    }
    pub fn wrap_with_egress(
        inner: Arc<dyn AsyncUdpSocket>,
        masks: &[Mask],
        server: bool,
        egress: Option<&zero_platform_tokio::EgressInterface>,
    ) -> io::Result<Arc<dyn AsyncUdpSocket>> {
        super::udp::validate(masks)?;
        if let Some(Mask::Xicmp { ip, id }) = masks.first() {
            let tunnel = super::xicmp::wrap(inner, ip, *id, server, egress)?;
            return Self::wrap_with_egress(tunnel, &masks[1..], server, egress);
        }
        if let Some(index) = masks
            .iter()
            .position(|mask| matches!(mask, Mask::Xdns { .. }))
        {
            let Mask::Xdns { domain } = &masks[index] else {
                unreachable!()
            };
            let outer = Self::wrap_with_egress(inner, &masks[..index], server, egress)?;
            let tunnel = super::xdns::wrap(outer, domain, server)?;
            return Self::wrap_with_egress(tunnel, &masks[index + 1..], server, egress);
        }
        if masks.is_empty() {
            return Ok(inner);
        }
        let sender = masks
            .iter()
            .any(|m| matches!(m, Mask::Noise(_)))
            .then(|| Codec::new(masks, server).map(|codec| send::Sender::new(inner.clone(), codec)))
            .transpose()?;
        Ok(Arc::new(Self {
            sender,
            inner,
            codec: Mutex::new(Codec::new(masks, server)?),
            pending: Mutex::new(Default::default()),
        }))
    }
}
impl AsyncUdpSocket for Socket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        match &self.sender {
            Some(sender) => sender.poller(),
            None => self.inner.clone().create_io_poller(),
        }
    }
    fn try_send(&self, transmit: &Transmit) -> io::Result<()> {
        if transmit.segment_size.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "FinalMask requires individual datagrams",
            ));
        }
        if let Some(sender) = &self.sender {
            return sender.send(transmit);
        }
        let bytes = self.codec.lock().unwrap().encode(transmit.contents)?;
        self.inner.try_send(&Transmit {
            destination: transmit.destination,
            ecn: transmit.ecn,
            contents: &bytes,
            segment_size: None,
            src_ip: transmit.src_ip,
        })
    }
    fn poll_recv(
        &self,
        cx: &mut Context,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>> {
        if let Some(sender) = &self.sender {
            sender.check(Some(cx))?;
        }
        if bufs.is_empty() || meta.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let mut pending = self.pending.lock().unwrap();
        let mut packet = [0u8; 65536];
        for _ in 0..32 {
            while let Some((decoded, info)) = pending.pop_front() {
                if decoded.len() > bufs[0].len() {
                    continue;
                }
                bufs[0][..decoded.len()].copy_from_slice(&decoded);
                meta[0] = RecvMeta {
                    len: decoded.len(),
                    stride: decoded.len(),
                    ..info
                };
                return Poll::Ready(Ok(1));
            }
            let mut input = [IoSliceMut::new(&mut packet)];
            let mut metadata = [RecvMeta::default()];
            let count = std::task::ready!(self.inner.poll_recv(cx, &mut input, &mut metadata))?;
            if count == 0 {
                return Poll::Ready(Ok(0));
            }
            let info = metadata[0];
            if info.stride == 0 || info.len > packet.len() {
                continue;
            }
            // Decode each member of a GRO batch independently. At most one
            // 64-KiB lower-socket batch is retained across upper-layer polls.
            let mut codec = self.codec.lock().unwrap();
            for encoded in packet[..info.len].chunks(info.stride) {
                if let Ok(decoded) = codec.decode(encoded) {
                    pending.push_back((decoded, info));
                }
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }
    fn may_fragment(&self) -> bool {
        self.inner.may_fragment()
    }
}

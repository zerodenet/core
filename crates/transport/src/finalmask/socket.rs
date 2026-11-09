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
mod receive;
mod send;
use super::memory::Memory;
pub struct Socket {
    sender: Option<send::Sender>,
    inner: Arc<dyn AsyncUdpSocket>,
    codec: Arc<Mutex<Codec>>,
    receive: receive::Receiver,
    memory: Memory,
}
impl std::fmt::Debug for Socket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FinalMaskSocket").finish_non_exhaustive()
    }
}
impl Socket {
    pub(super) fn prepare_codec(
        inner: Arc<dyn AsyncUdpSocket>,
        codec: Codec,
        first_noise: Option<usize>,
        memory: Memory,
        drivers: &mut Vec<super::owned::Driver>,
        offset: usize,
    ) -> io::Result<Arc<dyn AsyncUdpSocket>> {
        let codec = Arc::new(Mutex::new(codec));
        let sender = if let Some(index) = first_noise {
            let (sender, work) =
                send::Sender::prepare_owned(inner.clone(), codec.clone(), memory.clone());
            drivers.push(super::owned::Driver {
                kind: super::owned::DriverKind::Noise,
                ordinal: (offset + index) as u8,
                work,
            });
            Some(sender)
        } else {
            None
        };
        let receive = receive::Receiver::new(memory.scratch()?);
        Ok(Arc::new(Self {
            sender,
            inner,
            codec,
            receive,
            memory,
        }))
    }
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
        let codec = Arc::new(Mutex::new(Codec::new(masks, server)?));
        let sender = masks
            .iter()
            .any(|m| matches!(m, Mask::Noise(_)))
            .then(|| send::Sender::new(inner.clone(), codec.clone()));
        Ok(Arc::new(Self {
            sender,
            inner,
            codec,
            receive: receive::Receiver::new(Memory::default().scratch()?),
            memory: Memory::default(),
        }))
    }
}
impl AsyncUdpSocket for Socket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        match &self.sender {
            Some(sender) => sender.poller(),
            None => Box::pin(send::Poller::direct(
                self.inner.clone().create_io_poller(),
                self.memory.clone(),
            )),
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
        let _notifications = self.memory.metadata().map(|memory| memory.notifications());
        let peak = {
            self.codec
                .lock()
                .unwrap()
                .working_peak(transmit.contents.len())?
        };
        let _credit = self.memory.transmit(peak)?;
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
        self.receive.poll(self, cx, bufs, meta)
    }
    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }
    fn may_fragment(&self) -> bool {
        self.inner.may_fragment()
    }
}

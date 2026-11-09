use super::*;
use std::{
    future::Future,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use zero_platform_tokio::ClientStream;
use zero_traits::{AsyncSocket, TransportBypassControl};
type Driver = Pin<Box<dyn Future<Output = io::Result<()>> + Send>>;
type Wait = Pin<Box<dyn Future<Output = io::Result<CarrierBufferFrameCredit>> + Send>>;
pub(super) struct Stream {
    driver: std::sync::Mutex<Option<Driver>>,
    wait: std::sync::Mutex<Option<Wait>>,
    wake: wake::DriverWake,
    ready: Option<CarrierBufferFrameCredit>,
    pending: Option<queue::Frame>,
    offset: usize,
    shared: PacketMetadataArc<queue::Shared>,
    local: Option<SocketAddr>,
    peer: Option<SocketAddr>,
    bypass: Option<TransportBypassControl>,
}
impl Stream {
    pub(super) fn wrap(
        raw: TcpRelayStream,
        transform: super::super::stream::Transform,
        prepared: memory::Prepared,
        plan: PreparedMasks,
    ) -> io::Result<TcpRelayStream> {
        let local = raw.local_addr().ok();
        let peer = raw.peer_addr().ok();
        let bypass = raw.transport_bypass_control();
        let shared = PacketMetadataArc::from_prepaid(
            queue::Shared::new(prepared.memory),
            prepared.shared_header,
        )
        .unwrap_or_else(|_| unreachable!("same prepared inline mask state"));
        let driver = driver::run(driver::Input {
            raw,
            plan,
            transform,
            shared: shared.clone(),
            scratch: prepared.scratch,
            codec: prepared.codec,
        });
        let stream = Self {
            driver: std::sync::Mutex::new(Some(Box::pin(driver))),
            wait: std::sync::Mutex::new(None),
            wake: wake::DriverWake::new(prepared.wake_header),
            ready: None,
            pending: None,
            offset: 0,
            shared,
            local,
            peer,
            bypass,
        };
        TcpRelayStream::from_client_prepaid(stream, prepared.stream_header)
            .map_err(|_| io::Error::other("same prepared mask stream layout"))
    }
    fn drive(&mut self, cx: &mut Context<'_>, reading: bool) {
        self.wake.register(cx.waker(), reading);
        let Some(_polling) = self.wake.begin() else {
            return;
        };
        let driver = self
            .driver
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(future) = driver.as_mut() else {
            return;
        };
        let mut actual = Context::from_waker(self.wake.original());
        if let Poll::Ready(result) = future.as_mut().poll(&mut actual) {
            let old = driver.take();
            drop(old);
            if let Err(source) = result {
                self.shared.fail(source);
            }
        }
    }
}
impl AsyncRead for Stream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if output.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        self.drive(cx, true);
        if self.pending.is_none() {
            match std::task::ready!(self.shared.poll_input(cx))? {
                Some(frame) => {
                    self.pending = Some(frame);
                    self.offset = 0;
                }
                None => return Poll::Ready(Ok(())),
            }
        }
        let frame = self.pending.as_ref().expect("same original decoded frame");
        let n = (frame.bytes.len() - self.offset).min(output.remaining());
        output.put_slice(&frame.bytes[self.offset..self.offset + n]);
        self.offset += n;
        if self.offset == self.pending.as_ref().unwrap().bytes.len() {
            let old = self.pending.take();
            drop(old);
            self.offset = 0;
            self.drive(cx, true);
        }
        Poll::Ready(Ok(()))
    }
}
impl AsyncWrite for Stream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.drive(cx, false);
        std::task::ready!(self.shared.poll_output_slot(cx))?;
        if bytes.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let n = bytes.len().min(memory::WRITE);
        if self.ready.is_none() {
            let shared = self.shared.clone();
            let wait = self
                .wait
                .get_mut()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if wait.is_none() {
                *wait = Some(Box::pin(shared.memory.transmit.reserve(n)));
            }
            let credit = std::task::ready!(wait.as_mut().unwrap().as_mut().poll(cx))?;
            let old = wait.take();
            drop(old);
            self.ready = Some(credit);
        }
        let credit = self.ready.take().unwrap();
        // Cancellation may replace a pending write with a different slice.
        if credit.bytes() < n {
            drop(credit);
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        self.shared.push_output(queue::Frame {
            bytes: bytes[..n].to_vec(),
            _credit: credit,
        });
        self.drive(cx, false);
        Poll::Ready(Ok(n))
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.drive(cx, false);
        self.shared.poll_progress(cx, false)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.shared.close_output();
        self.drive(cx, false);
        self.shared.poll_progress(cx, true)
    }
}
impl zero_traits::AsyncSocket for Stream {
    type Error = io::Error;
    fn transport_bypass_control(&self) -> Option<TransportBypassControl> {
        self.bypass.clone()
    }
    async fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        tokio::io::AsyncReadExt::read(self, bytes).await
    }
    async fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        tokio::io::AsyncWriteExt::write_all(self, bytes).await
    }
    async fn shutdown(&mut self) -> io::Result<()> {
        tokio::io::AsyncWriteExt::shutdown(self).await
    }
}
impl ClientStream for Stream {
    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.local.ok_or_else(|| io::ErrorKind::Unsupported.into())
    }
    fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.peer.ok_or_else(|| io::ErrorKind::Unsupported.into())
    }
}

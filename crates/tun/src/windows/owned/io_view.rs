//! Sole packet view; the installed original owner keeps native and reader custody.
use super::*;
use std::{
    future::Future,
    net::IpAddr,
    pin::Pin,
    sync::atomic::Ordering,
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    sync::mpsc,
};

pub struct OwnedWintunIo {
    owner: Arc<OwnedWintunOwner>,
    name: String,
    configure: bool,
    identity: OwnedTunIdentity,
    rx: Option<mpsc::Receiver<io::Result<Vec<u8>>>>,
    write_retry: Option<Pin<Box<tokio::time::Sleep>>>,
}
impl OwnedWintunIo {
    pub(super) fn new(
        owner: Arc<OwnedWintunOwner>,
        name: String,
        configure: bool,
        identity: OwnedTunIdentity,
    ) -> Self {
        Self {
            owner,
            name,
            configure,
            identity,
            rx: None,
            write_retry: None,
        }
    }
    pub fn owned_identity(&self) -> &OwnedTunIdentity {
        &self.identity
    }
    fn start_reader(&mut self) -> io::Result<()> {
        if self.rx.is_some() {
            return Ok(());
        }
        let mut state = self.owner.state();
        if self.owner.closing.load(Ordering::Acquire)
            || state.reader.is_some()
            || state.reader_joined.is_some()
        {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "Wintun original reader revoked",
            ));
        }
        let session = state
            .session
            .clone()
            .ok_or_else(|| io::Error::other("Wintun original session missing"))?;
        let owner = self.owner.clone();
        let (tx, rx) = mpsc::channel(256);
        let reader = std::thread::Builder::new()
            .name("zero-owned-wintun".into())
            .spawn(move || loop {
                match session.receive_blocking() {
                    Ok(packet) => {
                        let bytes = packet.bytes().to_vec();
                        drop(packet);
                        if tx.blocking_send(Ok(bytes)).is_err() {
                            return Ok(());
                        }
                    }
                    Err(source) => {
                        if owner.closing.load(Ordering::Acquire) {
                            return Ok(());
                        }
                        return Err(io::Error::from(source));
                    }
                }
            })?;
        state.reader = Some(reader);
        self.rx = Some(rx);
        Ok(())
    }
}
impl AsyncRead for OwnedWintunIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if bytes.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        self.start_reader()?;
        match self.rx.as_mut().unwrap().poll_recv(cx) {
            Poll::Ready(Some(Ok(packet))) if packet.len() <= bytes.remaining() => {
                bytes.put_slice(&packet);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Some(Ok(_))) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "host packet receive buffer too small",
            ))),
            Poll::Ready(Some(Err(source))) => Poll::Ready(Err(source)),
            Poll::Ready(None) => {
                let state = self.owner.state();
                if let Some(reader) = state.reader.as_ref() {
                    if reader.is_finished() && !self.owner.closing.load(Ordering::Acquire) {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::BrokenPipe,
                            "original Wintun reader exited; exact source retained for join",
                        )));
                    }
                }
                Poll::Ready(Ok(()))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
impl AsyncWrite for OwnedWintunIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if bytes.is_empty() {
            return Poll::Ready(Ok(0));
        }
        if self.owner.closing.load(Ordering::Acquire) {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "Wintun original is closing",
            )));
        }
        if let Some(retry) = &mut self.write_retry {
            if retry.as_mut().poll(cx).is_pending() {
                return Poll::Pending;
            }
            self.write_retry = None;
        }
        let len = u16::try_from(bytes.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "oversized Wintun packet"))?;
        let session = self
            .owner
            .state()
            .session
            .clone()
            .ok_or_else(|| io::Error::other("original Wintun session missing"))?;
        let result = match session.allocate_send_packet(len) {
            Ok(mut packet) => {
                packet.bytes_mut().copy_from_slice(bytes);
                session.send_packet(packet);
                Poll::Ready(Ok(bytes.len()))
            }
            Err(source) if super::super::existing::buffer_full(&source) => {
                let mut retry = Box::pin(tokio::time::sleep(std::time::Duration::from_millis(1)));
                let _ = retry.as_mut().poll(cx);
                self.write_retry = Some(retry);
                Poll::Pending
            }
            Err(source) => Poll::Ready(Err(io::Error::from(source))),
        };
        result
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
impl crate::TunDevice for OwnedWintunIo {
    fn name(&self) -> &str {
        &self.name
    }
    fn configure(&self, addr: IpAddr, mask: IpAddr, mtu: u16) -> io::Result<()> {
        self.configure_addresses(&[(addr, mask)], mtu)
    }
    fn configure_addresses(&self, addresses: &[(IpAddr, IpAddr)], mtu: u16) -> io::Result<()> {
        if !self.configure {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "host-owned Wintun configuration is read-only",
            ));
        }
        super::super::configure_adapter(&self.name, addresses, mtu)
    }
}
impl Drop for OwnedWintunIo {
    fn drop(&mut self) {
        // This only returns the unique I/O view, waking a blocked delivery.
        // Native close and the original reader join remain in the same owner.
        drop(self.rx.take());
        self.owner.state().view_live = false;
    }
}

use super::*;
use std::{
    future::poll_fn,
    net::{IpAddr, SocketAddr},
    task::Poll,
};
use tokio::io::ReadBuf;
impl ControlledIcmpSocket {
    pub fn reservation(&self) -> IcmpReservationId {
        self.entry.id
    }
    pub fn mode(&self) -> IcmpMode {
        self.entry
            .metadata()
            .mode
            .expect("actual acquired ICMP mode")
    }
    pub fn raw_permission_source(&self) -> Option<Arc<std_io::Error>> {
        self.entry.metadata().raw_permission_source.clone()
    }
    fn with_live<T>(
        &self,
        action: impl FnOnce(&tokio::net::UdpSocket) -> std_io::Result<T>,
    ) -> std_io::Result<T> {
        let native = self.entry.native();
        if self.entry.cancel.is_cancelled() {
            return Err(retired());
        }
        match &*native {
            Native::Live(socket) => action(socket),
            _ => Err(unavailable(&native)),
        }
    }
    pub fn local_addr(&self) -> std_io::Result<IpAddr> {
        self.with_live(|socket| socket.local_addr().map(|address| address.ip()))
    }
    pub fn reply_identifier(&self, sent: u16) -> std_io::Result<u16> {
        self.with_live(|socket| {
            if self.mode() == IcmpMode::PingDatagram {
                socket.local_addr().map(|address| address.port())
            } else {
                Ok(sent)
            }
        })
    }
    pub async fn connect(&mut self, peer: IpAddr) -> std_io::Result<IpAddr> {
        self.check_family(peer)?;
        // Tokio's literal UDP connect uses this synchronous native operation;
        // do not hold the native mutex over an await or borrowed Tokio future.
        self.with_live(|socket| {
            socket2::SockRef::from(socket).connect(&SocketAddr::new(peer, 0).into())?;
            socket.local_addr().map(|address| address.ip())
        })
    }
    fn check_family(&self, peer: IpAddr) -> std_io::Result<()> {
        if peer.is_ipv6() == self.entry.ipv6 {
            Ok(())
        } else {
            Err(std_io::Error::new(
                std_io::ErrorKind::InvalidInput,
                "ICMP address family mismatch",
            ))
        }
    }
    pub async fn send(&mut self, bytes: &[u8]) -> std_io::Result<usize> {
        let waiter = self.entry.wakes.write.waiter();
        let cancelled = self.entry.cancel.clone().cancelled_owned();
        tokio::pin!(cancelled);
        poll_fn(|cx| {
            waiter.publish(cx.waker());
            let relay_waker = std::task::Waker::from(self.entry.wakes.write.clone());
            let cx = &mut std::task::Context::from_waker(&relay_waker);
            if std::future::Future::poll(cancelled.as_mut(), cx).is_ready() {
                return Poll::Ready(Err(retired()));
            }
            let native = self.entry.native();
            if self.entry.cancel.is_cancelled() {
                return Poll::Ready(Err(retired()));
            }
            match &*native {
                Native::Live(socket) => socket.poll_send(cx, bytes),
                _ => Poll::Ready(Err(unavailable(&native))),
            }
        })
        .await
    }
    pub async fn send_to(&mut self, bytes: &[u8], peer: IpAddr) -> std_io::Result<usize> {
        self.check_family(peer)?;
        let waiter = self.entry.wakes.write.waiter();
        let cancelled = self.entry.cancel.clone().cancelled_owned();
        tokio::pin!(cancelled);
        poll_fn(|cx| {
            waiter.publish(cx.waker());
            let relay_waker = std::task::Waker::from(self.entry.wakes.write.clone());
            let cx = &mut std::task::Context::from_waker(&relay_waker);
            if std::future::Future::poll(cancelled.as_mut(), cx).is_ready() {
                return Poll::Ready(Err(retired()));
            }
            let native = self.entry.native();
            if self.entry.cancel.is_cancelled() {
                return Poll::Ready(Err(retired()));
            }
            match &*native {
                Native::Live(socket) => socket.poll_send_to(cx, bytes, SocketAddr::new(peer, 0)),
                _ => Poll::Ready(Err(unavailable(&native))),
            }
        })
        .await
    }
    pub async fn recv_from(&mut self, bytes: &mut [u8]) -> std_io::Result<(usize, IpAddr)> {
        let waiter = self.entry.wakes.read.waiter();
        let cancelled = self.entry.cancel.clone().cancelled_owned();
        tokio::pin!(cancelled);
        loop {
            let (size, peer) = poll_fn(|cx| {
                waiter.publish(cx.waker());
                let relay_waker = std::task::Waker::from(self.entry.wakes.read.clone());
                let cx = &mut std::task::Context::from_waker(&relay_waker);
                if std::future::Future::poll(cancelled.as_mut(), cx).is_ready() {
                    return Poll::Ready(Err(retired()));
                }
                let native = self.entry.native();
                if self.entry.cancel.is_cancelled() {
                    return Poll::Ready(Err(retired()));
                }
                match &*native {
                    Native::Live(socket) => {
                        let mut buffer = ReadBuf::new(&mut *bytes);
                        match socket.poll_recv_from(cx, &mut buffer) {
                            Poll::Pending => Poll::Pending,
                            Poll::Ready(result) => {
                                Poll::Ready(result.map(|peer| (buffer.filled().len(), peer.ip())))
                            }
                        }
                    }
                    _ => Poll::Ready(Err(unavailable(&native))),
                }
            })
            .await?;
            if !self.entry.ipv6 && size > 0 && bytes[0] >> 4 == 4 {
                let header = usize::from(bytes[0] & 15) * 4;
                if header < 20 || header > size || bytes[9] != 1 {
                    continue;
                }
                bytes.copy_within(header..size, 0);
                return Ok((size - header, peer));
            }
            return Ok((size, peer));
        }
    }
}
fn retired() -> std_io::Error {
    std_io::Error::new(std_io::ErrorKind::NotConnected, "ICMP original is retired")
}
fn unavailable(native: &Native) -> std_io::Error {
    match native {
        Native::Unknown(source) => std_io::Error::new(source.kind(), Retained(source.clone())),
        _ => retired(),
    }
}
#[derive(Debug)]
struct Retained(Arc<std_io::Error>);
impl fmt::Display for Retained {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for Retained {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

//! Nonconsuming control-channel reads over the same accepted native original.
use super::*;
impl ControlledTcpStream {
    pub(crate) async fn peek_accepted(&self, bytes: &mut [u8]) -> io::Result<usize> {
        let waiter = self.shared.wakes.read.waiter();
        let cancelled = self.shared.cancel.clone().cancelled_owned();
        tokio::pin!(cancelled);
        std::future::poll_fn(|cx| {
            waiter.publish(cx.waker());
            let waker = std::task::Waker::from(self.shared.wakes.read.clone());
            let mut native = Context::from_waker(&waker);
            if cancelled.as_mut().poll(&mut native).is_ready() {
                return Poll::Ready(Err(closed()));
            }
            self.shared.poll_io(|socket| match socket {
                Socket::Tokio(socket) => {
                    let mut buffer = ReadBuf::new(bytes);
                    socket.inner.native()?.poll_peek(&mut native, &mut buffer)
                }
                #[cfg(any(unix, windows))]
                Socket::Native(_) => Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "accepted peek requires the original registered accepted stream",
                ))),
            })
        })
        .await
    }
}

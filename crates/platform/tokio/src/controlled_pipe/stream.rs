use super::*;
use std::pin::Pin;
impl ControlledPipe {
    pub async fn connect(&self) -> io::Result<()> {
        loop {
            let notified = self.shared.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let state = lock(&self.shared.state);
                if let Some(error) = &state.fault {
                    return Err(retained(error));
                }
                if self.shared.stop.load(Ordering::Acquire) || self.shared.scope_stop.is_cancelled()
                {
                    return Err(io::Error::new(
                        io::ErrorKind::Interrupted,
                        "pipe connect stopped",
                    ));
                }
                if state.connected {
                    return Ok(());
                }
            }
            notified.await;
        }
    }
}
impl AsyncRead for ControlledPipe {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if out.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let incoming = cx.waker().clone();
        let mut state = lock(&self.shared.state);
        if self.shared.stop.load(Ordering::Acquire) || self.shared.scope_stop.is_cancelled() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "original pipe read stopped",
            )));
        }
        if let Some(bytes) = state.read.front() {
            let start = state.read_offset;
            let size = out.remaining().min(bytes.len() - start);
            out.put_slice(&bytes[start..start + size]);
            if start + size == bytes.len() {
                state.read.pop_front();
                state.read_offset = 0;
            } else {
                state.read_offset += size;
            }
            drop(state);
            self.shared.wake_worker();
            return Poll::Ready(Ok(()));
        }
        if let Some(error) = &state.fault {
            return Poll::Ready(Err(retained(error)));
        }
        if state.eof || self.shared.stop.load(Ordering::Acquire) {
            return Poll::Ready(Ok(()));
        }
        let previous = state.read_waker.replace(incoming);
        drop(state);
        drop(previous); // External Waker drop can re-enter this same owner.
        Poll::Pending
    }
}
impl AsyncWrite for ControlledPipe {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let incoming = cx.waker().clone();
        let mut state = lock(&self.shared.state);
        if let Some(error) = &state.fault {
            return Poll::Ready(Err(retained(error)));
        }
        if state.write_closed
            || self.shared.stop.load(Ordering::Acquire)
            || self.shared.scope_stop.is_cancelled()
        {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "pipe output stopped",
            )));
        }
        if bytes.is_empty() {
            return Poll::Ready(Ok(0));
        }
        if state.write.len() == WRITE_SLOTS {
            let previous = state.write_waker.replace(incoming);
            drop(state);
            drop(previous); // External Waker drop can re-enter this same owner.
            return Poll::Pending;
        }
        let size = bytes.len().min(CHUNK);
        state.write.push_back(bytes[..size].to_vec());
        drop(state);
        self.shared.wake_worker();
        Poll::Ready(Ok(size))
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let incoming = cx.waker().clone();
        let mut state = lock(&self.shared.state);
        if let Some(error) = &state.fault {
            return Poll::Ready(Err(retained(error)));
        }
        if self.shared.stop.load(Ordering::Acquire) || self.shared.scope_stop.is_cancelled() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "original pipe flush stopped",
            )));
        }
        if state.write.is_empty() && !state.writing {
            return Poll::Ready(Ok(()));
        }
        let previous = state.flush_waker.replace(incoming);
        drop(state);
        drop(previous); // External Waker drop can re-enter this same owner.
        Poll::Pending
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.as_mut().poll_flush(cx) {
            Poll::Ready(Ok(())) => {
                lock(&self.shared.state).write_closed = true;
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}
impl Drop for ControlledPipe {
    fn drop(&mut self) {
        self.shared.request_stop();
        self.shared.view.store(false, Ordering::Release);
        self.shared.changed.notify_waiters();
    }
}

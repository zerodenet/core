//! Sender drain then the same original native flush, never enqueue-only END.
use super::*;
impl AsyncWrite for ScopedH2Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let progress = this.progress.clone();
        progress.writer.register(cx.waker());
        let _gate = progress.lock();
        let wake = waker_ref(&progress.writer);
        let mut context = Context::from_waker(&wake);
        let cx = &mut context;
        if let Some(source) = progress.failure.lock().unwrap().as_ref() {
            return Poll::Ready(Err(super::super::owned::retained(source)));
        }
        if this.ended {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        }
        if bytes.is_empty() {
            return Poll::Ready(Ok(0));
        }
        // At most one DATA frame is copied, and only after H2 grants capacity.
        let count = bytes.len().min(16 * 1024);
        this.send.reserve_capacity(count);
        let available = std::task::ready!(this.send.poll_capacity(cx))
            .ok_or_else(|| io::Error::from(io::ErrorKind::BrokenPipe))?
            .map_err(io::Error::other)?;
        if available == 0 {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        let n = count.min(available);
        this.send
            .send_data(Bytes::copy_from_slice(&bytes[..n]), false)
            .map_err(io::Error::other)?;
        this.target = None;
        Poll::Ready(Ok(n))
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let progress = this.progress.clone();
        progress.writer.register(cx.waker());
        let _gate = progress.lock();
        let wake = waker_ref(&progress.writer);
        let mut context = Context::from_waker(&wake);
        if let Some(source) = progress.failure.lock().unwrap().as_ref() {
            return Poll::Ready(Err(super::super::owned::retained(source)));
        }
        match this.send.poll_reset(&mut context) {
            Poll::Ready(Err(error)) => return Poll::Ready(Err(io::Error::other(error))),
            Poll::Ready(Ok(reason)) => {
                return Poll::Ready(Err(io::Error::other(h2::Error::from(reason))))
            }
            Poll::Pending => {}
        }
        // Stage one: all this sender's DATA and empty END/header/trailer
        // frames have actually left its h2 stream queue. An unrelated old
        // native flush cannot satisfy this condition.
        std::task::ready!(this.send.poll_send_drained(&mut context)).map_err(io::Error::other)?;
        // A successful terminal connection already passed codec flush/shutdown.
        // Cancellation/body exit alone never sets this actual result marker.
        if progress.completed.load(Ordering::Acquire) {
            return Poll::Ready(Ok(()));
        }
        let target = match this.target {
            Some(target) => target,
            None => {
                let target = progress
                    .flushed
                    .count
                    .load(Ordering::Acquire)
                    .saturating_add(1);
                this.target = Some(target);
                // Stage two: explicitly poll this same original connection
                // after drain observation, even when the codec is now idle.
                futures_util::task::ArcWake::wake_by_ref(&progress.driver);
                target
            }
        };
        progress.flushed.writer.register(&wake);
        progress.flushed.waiting.store(true, Ordering::Release);
        if let Some(source) = progress.failure.lock().unwrap().as_ref() {
            return Poll::Ready(Err(super::super::owned::retained(source)));
        }
        if progress.flushed.count.load(Ordering::Acquire) >= target {
            progress.flushed.waiting.store(false, Ordering::Release);
            Poll::Ready(Ok(()))
        } else if progress.ended.load(Ordering::Acquire) {
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "original scoped H2 driver ended before native flush",
            )))
        } else {
            Poll::Pending
        }
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        {
            let this = self.as_mut().get_mut();
            let progress = this.progress.clone();
            let _gate = progress.lock();
            if let Some(source) = progress.failure.lock().unwrap().as_ref() {
                return Poll::Ready(Err(super::super::owned::retained(source)));
            }
            if !this.ended {
                this.send
                    .send_data(Bytes::new(), true)
                    .map_err(io::Error::other)?;
                this.ended = true;
                this.target = None;
            }
        }
        self.poll_flush(cx)
    }
}

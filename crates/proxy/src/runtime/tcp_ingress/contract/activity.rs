use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

#[cfg(test)]
#[path = "activity/tests.rs"]
mod tests;

#[derive(Clone, Debug)]
pub(crate) struct TcpRelayActivity {
    state: Arc<ActivityState>,
}

pub(crate) struct TcpActivityStream<S> {
    inner: S,
    activity: TcpRelayActivity,
}

#[derive(Debug)]
struct ActivityState {
    epoch: Instant,
    last_activity_micros: AtomicU64,
}

impl TcpRelayActivity {
    pub(crate) fn new() -> Self {
        Self {
            state: Arc::new(ActivityState {
                epoch: Instant::now(),
                last_activity_micros: AtomicU64::new(0),
            }),
        }
    }

    pub(crate) fn touch(&self) {
        self.state
            .last_activity_micros
            .store(self.elapsed_micros(), Ordering::SeqCst);
    }

    pub(crate) async fn wait_for_idle(&self, timeout: Duration) {
        loop {
            let idle_for = self.idle_for();
            if idle_for >= timeout {
                return;
            }
            // Activity does not need to wake the watchdog. At the current
            // deadline it reloads the marker and sleeps to the refreshed one.
            tokio::time::sleep(timeout - idle_for).await;
        }
    }

    fn idle_for(&self) -> Duration {
        let last_activity = self.state.last_activity_micros.load(Ordering::SeqCst);
        Duration::from_micros(self.elapsed_micros().saturating_sub(last_activity))
    }

    fn elapsed_micros(&self) -> u64 {
        u64::try_from(self.state.epoch.elapsed().as_micros()).unwrap_or(u64::MAX)
    }
}

impl<S> TcpActivityStream<S> {
    pub(crate) fn new(inner: S, activity: TcpRelayActivity) -> Self {
        Self { inner, activity }
    }
}

impl<S> AsyncRead for TcpActivityStream<S>
where
    S: AsyncRead + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let filled_before = buffer.filled().len();
        let result = Pin::new(&mut self.inner).poll_read(cx, buffer);
        if matches!(&result, Poll::Ready(Ok(()))) && buffer.filled().len() > filled_before {
            self.activity.touch();
        }
        result
    }
}

impl<S> AsyncWrite for TcpActivityStream<S>
where
    S: AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        let result = Pin::new(&mut self.inner).poll_write(cx, buffer);
        if matches!(&result, Poll::Ready(Ok(written)) if *written != 0) {
            self.activity.touch();
        }
        result
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

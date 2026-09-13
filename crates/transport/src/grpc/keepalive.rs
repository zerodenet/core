//! Connection liveness runs alongside the H2 driver, never inside a protocol adapter.
use super::*;
use crate::profile::OwnedGrpcProfile;
use std::sync::{Arc, Mutex};
use tokio::time::{Duration, Instant};
#[derive(Clone)]
pub(super) struct Activity(Arc<Mutex<Instant>>);
impl Activity {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(Instant::now())))
    }
    fn touch(&self) {
        *self.0.lock().unwrap() = Instant::now();
    }
}
pub(super) struct ObservedIo<S> {
    pub io: S,
    pub activity: Activity,
}
impl<S: AsyncRead + Unpin> AsyncRead for ObservedIo<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let result = Pin::new(&mut self.io).poll_read(cx, buf);
        if buf.filled().len() > before {
            self.activity.touch();
        }
        result
    }
}
impl<S: AsyncWrite + Unpin> AsyncWrite for ObservedIo<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}
pub(super) async fn run(
    mut ping: h2::PingPong,
    profile: OwnedGrpcProfile,
    activity: Activity,
    server: bool,
    active: Arc<std::sync::atomic::AtomicUsize>,
) {
    // grpc-go clamps client keepalive to ten seconds. Zero means disabled on
    // clients and the gRPC two-hour default on servers.
    let seconds = if profile.idle_timeout_secs == 0 {
        if server {
            7200
        } else {
            std::future::pending::<()>().await;
            return;
        }
    } else if server {
        profile.idle_timeout_secs.max(1)
    } else {
        profile.idle_timeout_secs.max(10)
    };
    let interval = Duration::from_secs(seconds as u64);
    let deadline = Duration::from_secs(if profile.health_check_timeout_secs == 0 {
        20
    } else {
        profile.health_check_timeout_secs
    } as u64);
    loop {
        let last = *activity.0.lock().unwrap();
        tokio::time::sleep_until(last + interval).await;
        if Instant::now().duration_since(*activity.0.lock().unwrap()) < interval {
            continue;
        }
        if !server
            && !profile.permit_without_stream
            && active.load(std::sync::atomic::Ordering::Relaxed) == 0
        {
            tokio::time::sleep(interval).await;
            continue;
        }
        if !matches!(
            tokio::time::timeout(deadline, ping.ping(h2::Ping::opaque())).await,
            Ok(Ok(_))
        ) {
            return;
        }
        activity.touch();
    }
}

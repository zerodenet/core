//! One per-connection caller owns the original three futures, without a task.
//! Cached GrpcPool and explicit library task owners retain their own paths.
use super::*;
use crate::profile::OwnedGrpcProfile;
use futures_util::task::{waker_ref, ArcWake, AtomicWaker};
use std::sync::atomic::{AtomicBool, Ordering};
use std::{
    error::Error,
    fmt,
    sync::{Arc, Mutex},
};

pub(super) struct Inline(Mutex<State>);
struct State {
    driver: Option<SingleClientDriver>,
    failure: Option<Arc<io::Error>>,
    wake: Arc<WakeBoth>,
}
struct WakeBoth {
    reader: AtomicWaker,
    writer: AtomicWaker,
    ready: AtomicBool,
}
impl Default for WakeBoth {
    fn default() -> Self {
        Self {
            reader: AtomicWaker::new(),
            writer: AtomicWaker::new(),
            ready: AtomicBool::new(true),
        }
    }
}
impl ArcWake for WakeBoth {
    fn wake_by_ref(this: &Arc<Self>) {
        this.ready.store(true, Ordering::Release);
        this.reader.wake();
        this.writer.wake();
    }
}
impl Inline {
    pub(super) fn new(driver: SingleClientDriver) -> Self {
        Self(Mutex::new(State {
            driver: Some(driver),
            failure: None,
            wake: Arc::default(),
        }))
    }
    pub(super) fn poll(&mut self, cx: &mut Context<'_>, reading: bool) {
        // GrpcStream is exclusively borrowed by poll. The mutex only preserves
        // the existing Sync stream bound; no callout spans a metadata lock.
        let state = self.0.get_mut().expect("exclusive inline gRPC original");
        if reading {
            state.wake.reader.register(cx.waker());
        } else {
            state.wake.writer.register(cx.waker());
        }
        let Some(driver) = &mut state.driver else {
            return;
        };
        // Clear before polling; a real inline wake preserves the next turn.
        // Capacity-Pending by itself supplies no new native readiness.
        if !state.wake.ready.swap(false, Ordering::AcqRel) {
            return;
        }
        let wake = waker_ref(&state.wake);
        let mut driver_context = Context::from_waker(&wake);
        if let Poll::Ready(result) = driver.as_mut().poll(&mut driver_context) {
            if let Err(source) = result {
                state.failure = Some(Arc::new(source));
            }
            let finished = state.driver.take();
            drop(finished);
            state.wake.reader.wake();
            state.wake.writer.wake();
        }
    }
    pub(super) fn failure(&self) -> Option<io::Error> {
        self.0
            .lock()
            .unwrap()
            .failure
            .as_ref()
            .map(|source| io::Error::new(source.kind(), Original(source.clone())))
    }
}
#[derive(Debug)]
struct Original(Arc<io::Error>);
impl fmt::Display for Original {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.0.as_ref(), f)
    }
}
impl Error for Original {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.0.as_ref())
    }
}
/// Actual per-connection protocol owners drive this original connection while
/// polling their stream. This never opts an existing cached pool into inline IO.
pub async fn connect_grpc_inline_with_settings<S>(
    io: S,
    profile: &(impl zero_traits::GrpcTransportProfile + ?Sized),
    authority: &str,
    settings: Option<&[u8]>,
) -> Result<GrpcStream, RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let profile = OwnedGrpcProfile::from_profile(profile);
    let prepared = PreparedSingleClient::prepare(io, &profile, authority, settings).await?;
    let (mut stream, driver) = prepared.into_parts();
    stream.inline = Some(Inline::new(driver));
    Ok(stream)
}

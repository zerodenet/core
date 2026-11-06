//! The original client connection and application stream, before choosing its owner.
use super::*;
mod flush;
use flush::{FlushIo, Flushed};
use futures_util::task::AtomicWaker;
use std::{
    error::Error,
    fmt,
    future::Future,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
};
pub(super) type H2ClientDriver = Pin<Box<dyn Future<Output = io::Result<()>> + Send>>;
pub(super) struct PreparedH2Client {
    pub(super) stream: OwnedH2Client,
    pub(super) driver: H2ClientDriver,
}
pub(super) async fn prepare_request<S>(
    stream: S,
    request: Request<()>,
    settings: Option<&[u8]>,
) -> Result<PreparedH2Client, RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let flush = Arc::new(Flushed::default());
    let io = FlushIo {
        io: stream,
        flush: flush.clone(),
    };
    let mut builder = h2::client::Builder::new();
    if let Some(settings) = settings {
        builder.peer_application_settings(settings);
    }
    let (mut sender, connection) = builder.handshake(io).await.map_err(io::Error::other)?;
    let (response, send) = sender
        .send_request(request, false)
        .map_err(io::Error::other)?;
    let failure = Arc::new(Mutex::new(None));
    let driver_ended = Arc::new(AtomicBool::new(false));
    let ended = driver_ended.clone();
    let driver_failure = failure.clone();
    let driver_flush = flush.clone();
    let driver = Box::pin(async move {
        // Keep only this original request's sender; no application relay task.
        let _sender = sender;
        let result = match connection.await {
            Ok(()) => Ok(()),
            Err(error) => {
                let error = Arc::new(io::Error::other(error));
                *driver_failure.lock().unwrap() = Some(error.clone());
                Err(retained(&error))
            }
        };
        ended.store(true, Ordering::Release);
        if driver_flush.waiting.swap(false, Ordering::AcqRel) {
            driver_flush.writer.wake();
        }
        result
    });
    Ok(PreparedH2Client {
        stream: OwnedH2Client(Mutex::new(State {
            send,
            response: Some(response),
            recv: None,
            pending: Bytes::new(),
            write_buffer: bytes::BytesMut::new(),
            requested_remaining: 0,
            capacity_exhausted: false,
            ended: false,
            flush,
            target: 0,
            failure,
            read_failure: None,
            driver_ended,
        })),
        driver,
    })
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
fn retained(error: &Arc<io::Error>) -> io::Error {
    io::Error::new(error.kind(), Original(error.clone()))
}
/// Direct H2 flow control: copy at most one credited DATA frame per write.
/// The owning caller must also drive the original H2ClientDriver, or use the
/// existing connect_h2 API which does so inline in that same caller.
pub(super) struct OwnedH2Client(Mutex<State>);
struct State {
    send: h2::SendStream<Bytes>,
    response: Option<h2::client::ResponseFuture>,
    recv: Option<h2::RecvStream>,
    pending: Bytes,
    // Reuse the same allocation after its actual DATA frame is consumed.
    // This neither accepts uncredited bytes nor grows the H2 send window.
    write_buffer: bytes::BytesMut,
    // Remaining request intent only; actual capacity still gates every byte.
    requested_remaining: usize,
    // Negative observation only: never reuse a cached positive H2 grant.
    capacity_exhausted: bool,
    ended: bool,
    flush: Arc<Flushed>,
    target: u64,
    failure: Arc<Mutex<Option<Arc<io::Error>>>>,
    read_failure: Option<Arc<io::Error>>,
    // This observes the original future; it is never a native/user/join receipt.
    driver_ended: Arc<AtomicBool>,
}
#[derive(Debug)]
struct Reset(h2::Reason);
impl fmt::Display for Reset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "h2 stream reset: {}", self.0)
    }
}
impl Error for Reset {}
impl State {
    fn poll_reset(&mut self, cx: &mut Context<'_>) -> io::Result<()> {
        match self.send.poll_reset(cx) {
            Poll::Pending => Ok(()),
            Poll::Ready(Err(error)) => Err(io::Error::other(error)),
            Poll::Ready(Ok(reason)) => Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                Reset(reason),
            )),
        }
    }
    fn queued(&mut self) {
        self.target = self.flush.count.load(Ordering::Acquire).saturating_add(1);
    }
    fn failure(&self) -> Option<io::Error> {
        // Only this original driver's terminal branch writes failure. Its
        // Release publication follows that write, so live DATA polling need
        // not lock a diagnostics record for every credited fragment.
        if !self.driver_ended.load(Ordering::Acquire) {
            return None;
        }
        self.failure.lock().unwrap().as_ref().map(retained)
    }
}
impl Drop for State {
    fn drop(&mut self) {
        self.send.send_reset(h2::Reason::CANCEL);
    }
}
impl AsyncRead for OwnedH2Client {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut().0.get_mut().expect("exclusive H2 stream");
        if out.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if let Some(error) = &this.read_failure {
            return Poll::Ready(Err(retained(error)));
        }
        if let Some(response) = &mut this.response {
            let response = match Pin::new(response).poll(cx) {
                Poll::Pending => {
                    return match this.failure() {
                        Some(error) => Poll::Ready(Err(error)),
                        None => Poll::Pending,
                    };
                }
                Poll::Ready(result) => result,
            };
            this.response = None;
            match response {
                Ok(response) if response.status().is_success() => {
                    this.recv = Some(response.into_body())
                }
                result => {
                    let error = Arc::new(match result {
                        Ok(response) => io::Error::new(
                            io::ErrorKind::ConnectionAborted,
                            format!(
                                "h2 server returned non-success status {}",
                                response.status()
                            ),
                        ),
                        Err(error) => io::Error::other(error),
                    });
                    this.read_failure = Some(error.clone());
                    return Poll::Ready(Err(retained(&error)));
                }
            }
        }
        let recv = this.recv.as_mut().expect("original response consumed once");
        for _ in 0..32 {
            if !this.pending.is_empty() {
                let n = out.remaining().min(this.pending.len());
                out.put_slice(&this.pending.split_to(n));
                recv.flow_control()
                    .release_capacity(n)
                    .map_err(io::Error::other)?;
                return Poll::Ready(Ok(()));
            }
            match std::task::ready!(recv.poll_data(cx)) {
                Some(Ok(bytes)) => this.pending = bytes,
                Some(Err(error)) => return Poll::Ready(Err(io::Error::other(error))),
                None => return Poll::Ready(Ok(())),
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}
impl AsyncWrite for OwnedH2Client {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut().0.get_mut().expect("exclusive H2 stream");
        if this.driver_ended.load(Ordering::Acquire) {
            return Poll::Ready(Err(this
                .failure()
                .unwrap_or_else(|| io::ErrorKind::BrokenPipe.into())));
        }
        if this.ended {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        }
        if bytes.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let count = bytes.len().min(16 * 1024);
        if this.requested_remaining == 0 {
            this.send.reserve_capacity(count);
            this.requested_remaining = count;
        }
        // A newly delivered grant is the common path for a narrow real peer
        // window. Consume that notification directly, avoiding an extra
        // stream-store lock before every one-byte DATA frame. Pending still
        // needs the actual remaining-credit check: notifications report changes
        // and a previous successful write may have left credit available.
        let available = loop {
            match this.send.poll_capacity(cx) {
                Poll::Ready(Some(result)) => {
                    let available = result.map_err(io::Error::other)?;
                    if available != 0 {
                        break available;
                    }
                    this.capacity_exhausted = true;
                }
                Poll::Pending => {
                    // poll_capacity already registered this original stream's
                    // next credit/reset wake. If the previous real grant was
                    // consumed completely, Pending means it has not increased:
                    // do not lock the same h2 store again to rediscover zero.
                    if this.capacity_exhausted {
                        return Poll::Pending;
                    }
                    // A preceding write can leave assigned capacity without a
                    // new increase notification. Read that actual capacity;
                    // never treat a cached positive value as permission to IO.
                    let available = this.send.capacity();
                    if available != 0 {
                        break available;
                    }
                    this.capacity_exhausted = true;
                    // The preceding poll installed the real next-credit/reset
                    // waker; no uncredited application byte is accepted.
                    return Poll::Pending;
                }
                Poll::Ready(None) => {
                    this.poll_reset(cx)?;
                    return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
                }
            }
        };
        let n = count.min(available).min(this.requested_remaining);
        let frame = if n == 1 {
            // A narrow peer window can fragment DATA into individual bytes.
            // Those immutable values need neither a heap allocation nor a
            // shared allocation header. Actual H2 credit still gates n above.
            static VALUES: [u8; 256] = {
                let mut values = [0; 256];
                let mut index = 0;
                while index < 256 {
                    values[index] = index as u8;
                    index += 1;
                }
                values
            };
            let value = usize::from(bytes[0]);
            Bytes::from_static(&VALUES[value..value + 1])
        } else {
            this.write_buffer.reserve(n);
            this.write_buffer.extend_from_slice(&bytes[..n]);
            this.write_buffer.split().freeze()
        };
        if let Err(source) = this.send.send_data(frame, false) {
            // Sending already checks the same stream state. Preserve the
            // typed reset when present, without registering a redundant reset
            // waiter for each successful credited DATA frame.
            this.poll_reset(cx)?;
            return Poll::Ready(Err(io::Error::other(source)));
        }
        this.capacity_exhausted = n == available;
        this.requested_remaining -= n;
        this.queued();
        Poll::Ready(Ok(n))
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut().0.get_mut().expect("exclusive H2 stream");
        this.poll_reset(cx)?;
        this.flush.writer.register(cx.waker());
        this.flush.waiting.store(true, Ordering::Release);
        if let Some(error) = this.failure() {
            return Poll::Ready(Err(error));
        }
        if this.flush.count.load(Ordering::Acquire) >= this.target {
            this.flush.waiting.store(false, Ordering::Release);
            Poll::Ready(Ok(()))
        } else if this.driver_ended.load(Ordering::Acquire) {
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "original H2 connection ended before flush",
            )))
        } else {
            Poll::Pending
        }
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self
            .as_mut()
            .get_mut()
            .0
            .get_mut()
            .expect("exclusive H2 stream");
        if !this.ended {
            this.send
                .send_data(Bytes::new(), true)
                .map_err(io::Error::other)?;
            this.ended = true;
            this.queued();
        }
        self.poll_flush(cx)
    }
}

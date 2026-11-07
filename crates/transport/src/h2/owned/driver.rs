//! One original client Connection and its terminal result, polled by its caller.
use super::*;
trait ConnectionOriginal: Send + Unpin {
    fn poll_connection(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), h2::Error>>;
    fn poll_outbound(&mut self, cx: &mut Context<'_>) -> Poll<Result<bool, h2::Error>>;
}
impl<S> ConnectionOriginal for h2::client::Connection<FlushIo<S>, Bytes>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    fn poll_connection(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), h2::Error>> {
        Future::poll(Pin::new(self), cx)
    }
    fn poll_outbound(&mut self, cx: &mut Context<'_>) -> Poll<Result<bool, h2::Error>> {
        h2::client::Connection::poll_outbound(self, cx)
    }
}
pub(in crate::h2) struct OriginalDriver {
    connection: Box<dyn ConnectionOriginal>,
    _sender: h2::client::SendRequest<Bytes>,
    failure: Arc<Mutex<Option<Arc<io::Error>>>>,
    ended: Arc<AtomicBool>,
    flush: Arc<Flushed>,
    read: Arc<read::ReadSignal>,
}
impl OriginalDriver {
    pub(super) fn new<S>(
        connection: h2::client::Connection<FlushIo<S>, Bytes>,
        sender: h2::client::SendRequest<Bytes>,
        failure: Arc<Mutex<Option<Arc<io::Error>>>>,
        ended: Arc<AtomicBool>,
        flush: Arc<Flushed>,
        read: Arc<read::ReadSignal>,
    ) -> Self
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        Self {
            connection: Box::new(connection),
            _sender: sender,
            failure,
            ended,
            flush,
            read,
        }
    }
    fn finish(&self, result: Result<(), h2::Error>) -> io::Result<()> {
        let result = result.map_err(|source| {
            let source = Arc::new(io::Error::other(source));
            *self.failure.lock().unwrap() = Some(source.clone());
            retained(&source)
        });
        self.ended.store(true, Ordering::Release);
        self.flush.finish(result)
    }
    pub(in crate::h2) fn poll_outbound(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if !this.read.needs_full() {
            match this.connection.poll_outbound(cx) {
                Poll::Pending | Poll::Ready(Ok(true)) => return Poll::Pending,
                Poll::Ready(Err(source)) => return Poll::Ready(this.finish(Err(source))),
                Poll::Ready(Ok(false)) => {}
            }
        }
        // Actual pending/read wake, changed protocol state or a changed caller
        // requires the original full Future. Outbound success never ends it.
        Future::poll(Pin::new(this), cx)
    }
}
impl Future for OriginalDriver {
    type Output = io::Result<()>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.connection
            .poll_connection(cx)
            .map(|result| this.finish(result))
    }
}

impl Drop for OriginalDriver {
    fn drop(&mut self) {
        self.flush.interrupt();
    }
}

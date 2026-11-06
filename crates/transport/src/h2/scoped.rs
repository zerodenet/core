//! Raw H2 DATA over the original accepted carrier, without detached I/O pumps.
use super::*;
mod progress;
mod write;
use futures_util::task::waker_ref;
use progress::Progress;
use std::{
    sync::{atomic::Ordering, Arc},
    task::Context,
};
use zero_platform_tokio::{
    contract::{scope::TaskClass, CancellationToken, ChannelError, Impact, Reason, Stage},
    AcceptedCaller,
};

pub struct ScopedH2Stream {
    recv: h2::RecvStream,
    send: h2::SendStream<Bytes>,
    pending: Bytes,
    ended: bool,
    cancel: CancellationToken,
    progress: Arc<Progress>,
    target: Option<u64>,
}
impl Drop for ScopedH2Stream {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
pub async fn accept_h2_scoped<S>(
    stream: S,
    profile: &(impl H2TransportProfile + ?Sized),
    caller: &AcceptedCaller,
) -> Result<ScopedH2Stream, RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let progress = Arc::new(Progress::default());
    let io = super::owned::FlushIo {
        io: stream,
        flush: progress.flushed.clone(),
    };
    let mut conn = h2::server::handshake(io).await.map_err(io::Error::other)?;
    let (request, mut respond) = conn
        .accept()
        .await
        .ok_or_else(|| io::Error::from(io::ErrorKind::UnexpectedEof))?
        .map_err(io::Error::other)?;
    let expected = if profile.path().starts_with('/') {
        profile.path()
    } else {
        "/"
    };
    if request.uri().path() != expected {
        let response = Response::builder()
            .status(http::StatusCode::NOT_FOUND)
            .body(())
            .unwrap();
        respond
            .send_response(response, true)
            .map_err(io::Error::other)?;
        return Err(io::Error::new(
            io::ErrorKind::ConnectionRefused,
            format!(
                "h2 path mismatch: expected {expected}, got {}",
                request.uri().path()
            ),
        )
        .into());
    }
    let response = Response::builder()
        .header("content-type", "application/octet-stream")
        .body(())
        .unwrap();
    let send = respond
        .send_response(response, false)
        .map_err(io::Error::other)?;
    // Replace the handshake task's stored connection wake before the stream
    // can enqueue DATA inside the gate. This is one synchronous continuation
    // of the original accepted handshake, not another connection/task.
    {
        let wake = waker_ref(&progress.driver);
        let mut context = Context::from_waker(&wake);
        let _gate = progress.lock();
        if let Poll::Ready(result) = conn.poll_closed(&mut context) {
            result.map_err(io::Error::other)?;
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof).into());
        }
    }
    let cancel = CancellationToken::new();
    let stopping = cancel.clone();
    let actual = progress.clone();
    caller
        .spawn(TaskClass::Auxiliary, move |_| async move {
            let _ended = progress::Ended(actual.clone());
            loop {
                let next = tokio::select! { biased;
                    _=stopping.cancelled()=>return Ok(()),
                    next=std::future::poll_fn(|cx| {
                        actual.driver.register(cx.waker());
                        let wake = waker_ref(&actual.driver);
                        let mut context = Context::from_waker(&wake);
                        let _gate = actual.lock();
                        conn.poll_accept(&mut context)
                    })=>next,
                };
                match next {
                    None => {
                        actual.completed.store(true, Ordering::Release);
                        return Ok(());
                    }
                    Some(Err(error)) => {
                        let source = Arc::new(io::Error::other(error));
                        *actual.failure.lock().unwrap() = Some(source.clone());
                        return Err(
                            ChannelError::new(Stage::Receive, Reason::Io, Impact::Session)
                                .with_source(super::owned::retained(&source)),
                        );
                    }
                    Some(Ok((_, mut respond))) => {
                        // Existing H2 carrier admits one application stream per
                        // connection; preserve its response to subsequent requests.
                        let response = Response::builder()
                            .status(http::StatusCode::SERVICE_UNAVAILABLE)
                            .body(())
                            .unwrap();
                        let _gate = actual.lock();
                        let _ = respond.send_response(response, true);
                    }
                }
            }
        })
        .map_err(io::Error::other)?;
    Ok(ScopedH2Stream {
        recv: request.into_body(),
        send,
        pending: Bytes::new(),
        ended: false,
        cancel,
        progress,
        target: None,
    })
}
impl AsyncRead for ScopedH2Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let progress = this.progress.clone();
        progress.reader.register(cx.waker());
        let _gate = progress.lock();
        let wake = waker_ref(&progress.reader);
        let mut context = Context::from_waker(&wake);
        let cx = &mut context;
        if out.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        for _ in 0..32 {
            if !this.pending.is_empty() {
                let n = out.remaining().min(this.pending.len());
                out.put_slice(&this.pending.split_to(n));
                this.recv
                    .flow_control()
                    .release_capacity(n)
                    .map_err(io::Error::other)?;
                return Poll::Ready(Ok(()));
            }
            match std::task::ready!(this.recv.poll_data(cx)) {
                Some(Ok(data)) => this.pending = data,
                Some(Err(error)) => return Poll::Ready(Err(io::Error::other(error))),
                None => return Poll::Ready(Ok(())),
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}
impl AsyncSocket for ScopedH2Stream {
    type Error = io::Error;
    async fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        AsyncReadExt::read(self, bytes).await
    }
    async fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        AsyncWriteExt::write_all(self, bytes).await
    }
    async fn shutdown(&mut self) -> io::Result<()> {
        AsyncWriteExt::shutdown(self).await
    }
}
impl ClientStream for ScopedH2Stream {}

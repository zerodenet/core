//! Raw H2 DATA over the original accepted carrier, without detached I/O pumps.
use super::*;
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
    let mut conn = h2::server::handshake(stream)
        .await
        .map_err(io::Error::other)?;
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
    let cancel = CancellationToken::new();
    let stopping = cancel.clone();
    caller
        .spawn(TaskClass::Auxiliary, move |_| async move {
            loop {
                let next = tokio::select! { biased;
                    _=stopping.cancelled()=>return Ok(()),
                    next=conn.accept()=>next,
                };
                match next {
                    None => return Ok(()),
                    Some(Err(error)) => {
                        return Err(
                            ChannelError::new(Stage::Receive, Reason::Io, Impact::Session)
                                .with_source(error),
                        )
                    }
                    Some(Ok((_, mut respond))) => {
                        // Existing H2 carrier admits one application stream per
                        // connection; preserve its response to subsequent requests.
                        let response = Response::builder()
                            .status(http::StatusCode::SERVICE_UNAVAILABLE)
                            .body(())
                            .unwrap();
                        let _ = respond.send_response(response, true);
                    }
                }
            }
        })
        .map_err(|error| io::Error::other(error))?;
    Ok(ScopedH2Stream {
        recv: request.into_body(),
        send,
        pending: Bytes::new(),
        ended: false,
        cancel,
    })
}
impl AsyncRead for ScopedH2Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
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
impl AsyncWrite for ScopedH2Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
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
        Poll::Ready(Ok(n))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        // DATA was handed to this same H2 connection under its flow-control
        // credit. There is no independent application write queue to drain.
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if !this.ended {
            this.send
                .send_data(Bytes::new(), true)
                .map_err(io::Error::other)?;
            this.ended = true;
        }
        Poll::Ready(Ok(()))
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

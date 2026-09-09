//! HTTP/1 and HTTP/2 carriers. Listener ownership stays with the calling runtime.
use bytes::Bytes;
use http_body_util::BodyExt;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use std::{
    io,
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{mpsc, oneshot, Semaphore};
mod body;
use body::ResponseBody;

#[derive(Clone, Copy)]
pub struct RequestContext {
    pub peer: SocketAddr,
    pub tls: bool,
}
#[async_trait::async_trait]
pub trait HttpExchange: Send {
    async fn recv_data(&mut self) -> io::Result<Option<Bytes>>;
    async fn send_response(&mut self, response: http::Response<()>) -> io::Result<()>;
    async fn send_data(&mut self, data: Bytes) -> io::Result<()>;
    async fn finish(&mut self) -> io::Result<()>;
}
#[async_trait::async_trait]
pub trait HttpHandler: Send + Sync + 'static {
    async fn serve(
        &self,
        request: http::Request<()>,
        exchange: &mut dyn HttpExchange,
    ) -> io::Result<()>;
}
struct Exchange {
    body: hyper::body::Incoming,
    headers: Option<oneshot::Sender<http::Response<()>>>,
    sender: mpsc::Sender<io::Result<Bytes>>,
}
#[async_trait::async_trait]
impl HttpExchange for Exchange {
    async fn recv_data(&mut self) -> io::Result<Option<Bytes>> {
        while let Some(frame) = self.body.frame().await {
            if let Ok(data) = frame.map_err(io::Error::other)?.into_data() {
                return Ok(Some(data));
            }
        }
        Ok(None)
    }
    async fn send_response(&mut self, response: http::Response<()>) -> io::Result<()> {
        self.headers
            .take()
            .ok_or_else(|| io::Error::other("response already started"))?
            .send(response)
            .map_err(|_| io::Error::other("HTTP request closed"))
    }
    async fn send_data(&mut self, data: Bytes) -> io::Result<()> {
        self.sender
            .send(Ok(data))
            .await
            .map_err(|_| io::Error::other("HTTP response closed"))
    }
    async fn finish(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub async fn serve_connection<S>(
    stream: S,
    context: RequestContext,
    handler: Arc<dyn HttpHandler>,
) -> io::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let permits = Arc::new(Semaphore::new(64));
    let service =
        hyper::service::service_fn(move |request: http::Request<hyper::body::Incoming>| {
            let handler = handler.clone();
            let permit = permits.clone().try_acquire_owned();
            async move {
                let (headers_tx, headers_rx) = oneshot::channel();
                let (tx, rx) = mpsc::channel(8);
                let (mut parts, body) = request.into_parts();
                parts.extensions.insert(context);
                let failed = Arc::new(AtomicBool::new(false));
                let task_failed = failed.clone();
                let task = tokio::spawn(async move {
                    let _permit = match permit {
                        Ok(permit) => permit,
                        Err(_) => {
                            let _ = headers_tx
                                .send(http::Response::builder().status(503).body(()).unwrap());
                            return;
                        }
                    };
                    let mut exchange = Exchange {
                        body,
                        headers: Some(headers_tx),
                        sender: tx,
                    };
                    let result = tokio::time::timeout(
                        Duration::from_secs(30),
                        handler.serve(http::Request::from_parts(parts, ()), &mut exchange),
                    )
                    .await;
                    if !matches!(result, Ok(Ok(()))) {
                        task_failed.store(true, Ordering::Release);
                    }
                });
                // The body owns cancellation even while the handler is still producing headers.
                let body = ResponseBody {
                    receiver: rx,
                    task: task.abort_handle(),
                    failed,
                };
                let response = headers_rx
                    .await
                    .map_err(|_| io::Error::other("HTTP response unavailable"))?;
                Ok::<_, io::Error>(response.map(|_| body))
            }
        });
    let mut builder = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new());
    builder
        .http1()
        .timer(TokioTimer::new())
        .header_read_timeout(Duration::from_secs(30));
    builder
        .http2()
        .max_concurrent_streams(64)
        .timer(TokioTimer::new())
        .keep_alive_interval(Duration::from_secs(30))
        .keep_alive_timeout(Duration::from_secs(10));
    builder
        .serve_connection(TokioIo::new(stream), service)
        .await
        .map_err(io::Error::other)
}

#[cfg(test)]
mod tests;

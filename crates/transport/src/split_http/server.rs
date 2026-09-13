//! HTTP request execution and transport stream delivery, without routing.
use super::{
    body::Body, io::XhttpStream, request::Profile, sessions::Session, SplitHttpRegistry, XhttpMode,
};
use bytes::Bytes;
use http_body_util::BodyExt;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use std::{io, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::mpsc,
};
use zero_traits::SplitHttpTransportProfile;
pub(super) mod exchange;
mod http1;
mod packet;

pub struct XhttpIncoming {
    pub(super) receiver: tokio::sync::Mutex<mpsc::Receiver<XhttpStream>>,
    pub(super) task: Option<tokio::task::AbortHandle>,
}
impl Drop for XhttpIncoming {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}
impl XhttpIncoming {
    pub async fn accept(&self) -> Option<XhttpStream> {
        self.receiver.lock().await.recv().await
    }
    pub fn close(&self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
    pub(super) async fn first(mut self) -> Option<XhttpStream> {
        let stream = self.accept().await?;
        if let Some(task) = self.task.take() {
            let life = stream.life.clone();
            tokio::spawn(async move {
                life.cancelled().await;
                task.abort();
            });
        }
        Some(stream)
    }
}
impl zero_core::InboundTransportMultiplexer for XhttpIncoming {
    type Stream = XhttpStream;
    async fn accept_stream(&self) -> Option<Self::Stream> {
        self.accept().await
    }
    fn close(&self) {
        self.close();
    }
}

pub fn accept_xhttp_connection<S, P>(
    stream: S,
    config: &P,
    registry: &SplitHttpRegistry,
) -> XhttpIncoming
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    P: SplitHttpTransportProfile + ?Sized,
{
    let profile = Profile::new(config);
    let max_header_bytes = profile.options.server_max_header_bytes;
    let sessions = registry.sessions.clone();
    let (sender, receiver) = mpsc::channel(64);
    let permits = registry.requests.clone();
    let policy = http1::ReplyPolicy::default();
    let stream = http1::UploadDrain::new(stream, policy.clone());
    let task = tokio::spawn(async move {
        let service =
            hyper::service::service_fn(move |mut request: http::Request<hyper::body::Incoming>| {
                policy.set_packet(false);
                request.extensions_mut().insert(policy.clone());
                let profile = profile.clone();
                let sessions = sessions.clone();
                let sender = sender.clone();
                let permit = permits.clone().try_acquire_owned();
                async move {
                    let response = match permit {
                        Ok(permit) => {
                            let response =
                                exchange::handle(request, profile, sessions, sender, permit).await;
                            response.unwrap_or_else(|error| {
                                tracing::debug!(%error,"XHTTP request rejected");
                                status(400)
                            })
                        }
                        Err(_) => status(503),
                    };
                    Ok::<_, io::Error>(response)
                }
            });
        let mut builder = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new());
        builder
            .http1()
            .half_close(true)
            .max_buf_size((max_header_bytes as usize).max(8192))
            .timer(TokioTimer::new())
            .header_read_timeout(Duration::from_secs(30));
        builder
            .http2()
            .max_concurrent_streams(64)
            .max_header_list_size(max_header_bytes)
            .timer(TokioTimer::new());
        if let Err(error) = builder
            .serve_connection(TokioIo::new(stream), service)
            .await
        {
            tracing::debug!(?error, "XHTTP HTTP connection failed");
        }
    });
    XhttpIncoming {
        receiver: tokio::sync::Mutex::new(receiver),
        task: Some(task.abort_handle()),
    }
}
pub(super) fn status(code: u16) -> http::Response<Body> {
    http::Response::builder()
        .status(code)
        .body(Body::empty())
        .unwrap()
}
fn response(body: Body) -> http::Response<Body> {
    http::Response::builder()
        .status(200)
        .header("cache-control", "no-store")
        .header("x-accel-buffering", "no")
        .header("content-type", "text/event-stream")
        .body(body)
        .unwrap()
}

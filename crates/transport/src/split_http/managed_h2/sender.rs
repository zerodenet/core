use super::failure::Failure;
use hyper::body::Body as _;
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use zero_platform_tokio::contract::{ChannelError, DeliveryState, Reason, Stage};

/// The raw sender cannot escape the shared first-failure observer.
pub struct Sender<B> {
    sender: hyper::client::conn::http2::SendRequest<B>,
    failure: Arc<Failure>,
}
impl<B> Sender<B> {
    pub(super) fn new(
        sender: hyper::client::conn::http2::SendRequest<B>,
        failure: Arc<Failure>,
    ) -> Self {
        Self { sender, failure }
    }
    pub async fn ready(&mut self) -> Result<(), Arc<ChannelError>> {
        self.failure
            .run(Stage::Handshake, self.sender.ready())
            .await
    }
    /// Sends the original request once. Failure never retries business bytes.
    pub async fn send_request(
        &mut self,
        request: http::Request<B>,
    ) -> Result<http::Response<ResponseBody>, Arc<ChannelError>>
    where
        B: hyper::body::Body + 'static,
    {
        if request.method() == http::Method::CONNECT {
            let mut source = self.failure.local(
                Stage::Prepare,
                Reason::Unsupported,
                std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "managed XHTTP H2 does not admit CONNECT upgrades",
                ),
            );
            Arc::get_mut(&mut source).expect("new local error").delivery =
                DeliveryState::NotSubmitted;
            return Err(source);
        }
        let failure = self.failure.clone();
        // Construct send_request only after admission, since construction
        // submits the original request to Hyper before its future is polled.
        if let Some(source) = failure.get() {
            return Err(source);
        }
        let response = failure
            .run(Stage::Handshake, self.sender.send_request(request))
            .await?;
        Ok(response.map(|body| ResponseBody::new(body, failure)))
    }
}
impl<B> Clone for Sender<B> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            failure: self.failure.clone(),
        }
    }
}

/// Incoming body and failure notifications share the exact connection error Arc.
/// Each body has its own registered waiter; one AtomicWaker would lose siblings.
pub struct ResponseBody {
    body: hyper::body::Incoming,
    failure: Arc<Failure>,
    waiting: Pin<Box<dyn Future<Output = Arc<ChannelError>> + Send>>,
    finished: bool,
}
impl ResponseBody {
    fn new(body: hyper::body::Incoming, failure: Arc<Failure>) -> Self {
        let notified = failure.clone();
        Self {
            body,
            failure,
            waiting: Box::pin(async move { notified.wait().await }),
            finished: false,
        }
    }
}
impl hyper::body::Body for ResponseBody {
    type Data = bytes::Bytes;
    type Error = Arc<ChannelError>;
    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        if this.finished {
            return Poll::Ready(None);
        }
        if let Poll::Ready(source) = this.waiting.as_mut().poll(cx) {
            this.finished = true;
            return Poll::Ready(Some(Err(source)));
        }
        match Pin::new(&mut this.body).poll_frame(cx) {
            Poll::Ready(None) => {
                this.finished = true;
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(source))) => {
                this.finished = true;
                Poll::Ready(Some(Err(this.failure.get().unwrap_or_else(|| {
                    this.failure.local(Stage::Receive, Reason::Io, source)
                }))))
            }
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(source) = this.failure.get() {
                    this.finished = true;
                    Poll::Ready(Some(Err(source)))
                } else {
                    Poll::Ready(Some(Ok(frame)))
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
    fn is_end_stream(&self) -> bool {
        self.finished || self.body.is_end_stream()
    }
    fn size_hint(&self) -> hyper::body::SizeHint {
        self.body.size_hint()
    }
}

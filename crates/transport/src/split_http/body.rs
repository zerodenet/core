mod receiver;
use super::io::LifetimeRef;
use bytes::Bytes;
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::sync::mpsc;

pub(super) enum Task {
    Legacy(tokio::task::AbortHandle),
    Scoped(zero_platform_tokio::contract::CancellationToken),
}
impl From<tokio::task::AbortHandle> for Task {
    fn from(h: tokio::task::AbortHandle) -> Self {
        Self::Legacy(h)
    }
}
impl Task {
    pub(super) fn abort(&self) {
        match self {
            Self::Legacy(h) => h.abort(),
            Self::Scoped(c) => c.cancel(),
        }
    }
}
#[derive(Clone)]
pub(in crate::split_http) struct Caller(pub zero_platform_tokio::AcceptedCaller);
pub(super) fn spawn(
    caller: Option<&Caller>,
    work: impl std::future::Future<Output = ()> + Send + 'static,
) -> io::Result<Task> {
    match caller {
        None => Ok(Task::Legacy(tokio::spawn(work).abort_handle())),
        Some(caller) => {
            let cancel = zero_platform_tokio::contract::CancellationToken::new();
            let stopped = cancel.clone();
            caller
                .0
                .spawn(
                    zero_platform_tokio::contract::lifecycle::TaskClass::Auxiliary,
                    move |_| async move {
                        tokio::select! {biased;_=stopped.cancelled()=>{},_=work=>{}}
                        Ok(())
                    },
                )
                .map_err(io::Error::other)?;
            Ok(Task::Scoped(cancel))
        }
    }
}
pub(super) struct Body {
    pub(super) receiver: receiver::Receiver,
    pub(super) task: Option<Task>,
    pub(super) life: Option<LifetimeRef>,
    pub(super) progress: Option<LifetimeRef>,
}
impl Body {
    pub(super) fn empty() -> Self {
        let (_, receiver) = mpsc::channel(1);
        Self {
            receiver: receiver.into(),
            task: None,
            life: None,
            progress: None,
        }
    }
    pub(super) fn full(bytes: Bytes) -> Self {
        let (sender, receiver) = mpsc::channel(1);
        sender.try_send(Ok(bytes)).unwrap();
        Self {
            receiver: receiver.into(),
            task: None,
            life: None,
            progress: None,
        }
    }
}
impl hyper::body::Body for Body {
    type Data = Bytes;
    type Error = io::Error;
    fn is_end_stream(&self) -> bool {
        self.receiver.is_closed() && self.receiver.is_empty()
    }
    fn size_hint(&self) -> hyper::body::SizeHint {
        if self.receiver.is_closed() && self.receiver.is_empty() {
            hyper::body::SizeHint::with_exact(0)
        } else {
            hyper::body::SizeHint::default()
        }
    }
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Bytes>, io::Error>>> {
        let result = self.receiver.poll_recv(cx);
        if let Poll::Ready(Some(Ok(data))) = &result {
            if let Some(progress) = &self.progress {
                progress.commit(data.len());
            }
        }
        result.map(|item| item.map(|result| result.map(hyper::body::Frame::data)))
    }
}
impl Drop for Body {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
        if let Some(life) = &self.life {
            tracing::debug!(state = ?life.as_ptr(), "XHTTP response ownership ended");
            life.close();
        }
    }
}

pub(super) enum ReceivedBody {
    Http(hyper::body::Incoming),
    H3(Body),
    OwnedH3(super::xmux::owned::request::PaidResponseBody),
    Guarded(Box<GuardedBody>),
    OwnedGroup(super::xmux::group_body::OwnedGroupBody),
}
impl hyper::body::Body for ReceivedBody {
    type Data = Bytes;
    type Error = io::Error;
    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Bytes>, io::Error>>> {
        match self.get_mut() {
            Self::Http(body) => Pin::new(body)
                .poll_frame(cx)
                .map(|frame| frame.map(|f| f.map_err(io::Error::other))),
            Self::Guarded(body) => Pin::new(body.as_mut()).poll_frame(cx),
            Self::OwnedGroup(body) => Pin::new(body).poll_frame(cx),
            Self::H3(body) => Pin::new(body).poll_frame(cx),
            Self::OwnedH3(body) => Pin::new(body).poll_frame(cx),
        }
    }
}

pub(super) struct GuardedBody {
    body: ReceivedBody,
    release: Option<Box<dyn FnOnce(bool) + Send>>,
}
impl ReceivedBody {
    pub(super) fn guarded(self, release: impl FnOnce(bool) + Send + 'static) -> Self {
        Self::Guarded(Box::new(GuardedBody {
            body: self,
            release: Some(Box::new(release)),
        }))
    }
}
impl hyper::body::Body for GuardedBody {
    type Data = Bytes;
    type Error = io::Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<io::Result<hyper::body::Frame<Bytes>>>> {
        let frame = Pin::new(&mut self.body).poll_frame(cx);
        if matches!(frame, Poll::Ready(None)) {
            if let Some(release) = self.release.take() {
                release(true);
            }
        }
        frame
    }
}
impl Drop for GuardedBody {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            release(false);
        }
    }
}

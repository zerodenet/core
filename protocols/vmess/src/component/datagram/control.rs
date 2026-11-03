use super::*;
use std::{future::poll_fn, pin::Pin, task::Poll};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    time::{timeout_at, Instant},
};
use zero_platform_tokio::contract::{
    lifecycle::{ChannelControl, CloseReport, Unfinished},
    CloseReason,
};
pub struct Control {
    shared: Arc<Shared>,
    shutdown: bool,
    closed: bool,
}
impl Control {
    pub(in crate::component) async fn return_carrier_before(&mut self, deadline: Instant) -> bool {
        self.cancel(CloseReason::Shutdown);
        let shared = self.shared.clone();
        timeout_at(deadline, async {
            loop {
                let changed = shared.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.return_carrier() {
                    return;
                }
                changed.await;
            }
        })
        .await
        .is_ok()
    }
    /// Return logical custody only. The gRPC controller must separately consume
    /// its original driver and native close receipts before reporting closure.
    pub(in crate::component) fn return_carrier(&mut self) -> bool {
        let mut state = self.shared.state.lock().unwrap();
        if state.views != 0 {
            return false;
        }
        let stream = state.stream.take();
        drop(state);
        drop(stream);
        self.closed = true;
        true
    }
    pub(super) fn new(shared: Arc<Shared>) -> Self {
        Self {
            shared,
            shutdown: false,
            closed: false,
        }
    }
}
impl ChannelControl for Control {
    fn cancel(&self, _: CloseReason) {
        self.shared.cancel.cancel();
        self.shared.changed.notify_waiters();
    }
    async fn wait_closed(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.cancel(CloseReason::Shutdown);
        if self.closed {
            return Ok(CloseReport::default());
        }
        let shared = self.shared.clone();
        let close = async {
            loop {
                let changed = shared.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if shared.state.lock().unwrap().views == 0 {
                    break;
                }
                changed.await;
            }
            poll_fn(|cx| {
                let mut state = shared.state.lock().unwrap();
                let stream = state.stream.as_mut().expect("retained VMess UDP original");
                if !self.shutdown {
                    std::task::ready!(Pin::new(&mut *stream).poll_shutdown(cx)).map_err(
                        |source| {
                            shared
                                .error(Stage::Close, Reason::Io, DeliveryState::Unknown)
                                .with_source(source)
                        },
                    )?;
                    self.shutdown = true;
                }
                let mut bytes = [0; 4096];
                let mut buf = ReadBuf::new(&mut bytes);
                std::task::ready!(Pin::new(stream).poll_read(cx, &mut buf)).map_err(|source| {
                    shared
                        .error(Stage::Close, Reason::Io, DeliveryState::Unknown)
                        .with_source(source)
                })?;
                if buf.filled().is_empty() {
                    drop(state.stream.take());
                    Poll::Ready(Ok::<_, ChannelError>(()))
                } else {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            })
            .await
        };
        match timeout_at(deadline, close).await {
            Ok(Err(error)) => Err(error),
            Ok(Ok(())) => {
                self.closed = true;
                Ok(CloseReport {
                    closed_io: vec!["VMess UDP logical stream".into()],
                    ..Default::default()
                })
            }
            Err(_) => Ok(CloseReport {
                unfinished: vec![Unfinished::Io(
                    "VMess UDP original stream or transferred views".into(),
                )],
                ..Default::default()
            }),
        }
    }
}
impl Drop for Control {
    fn drop(&mut self) {
        self.shared.cancel.cancel();
    }
}

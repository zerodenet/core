use super::WebSocketSocket;
use futures_util::SinkExt;
use std::{
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    time::{Instant, Sleep},
};
use tokio_tungstenite::tungstenite::Message;

pub(super) struct Heartbeat {
    interval: Duration,
    deadline: Pin<Box<Sleep>>,
    flushing: bool,
}
impl<S> WebSocketSocket<S> {
    pub(crate) fn set_heartbeat(&mut self, seconds: u32) {
        self.heartbeat = (seconds > 0).then(|| {
            let interval = Duration::from_secs(u64::from(seconds));
            Heartbeat {
                interval,
                deadline: Box::pin(tokio::time::sleep(interval)),
                flushing: false,
            }
        });
    }
}
impl<S: AsyncRead + AsyncWrite + Unpin> WebSocketSocket<S> {
    // Both I/O directions drive one persistent timer. Never block reads behind
    // a pending control write: that can deadlock peers under backpressure.
    pub(super) fn poll_heartbeat(&mut self, cx: &mut Context<'_>) -> io::Result<()> {
        let Some(heartbeat) = self.heartbeat.as_mut() else {
            return Ok(());
        };
        if !heartbeat.flushing && heartbeat.deadline.as_mut().poll(cx).is_ready() {
            match self.inner.poll_ready_unpin(cx) {
                Poll::Pending => return Ok(()),
                Poll::Ready(result) => result.map_err(io::Error::other)?,
            }
            self.inner
                .start_send_unpin(Message::Ping(Vec::new()))
                .map_err(io::Error::other)?;
            heartbeat.flushing = true;
        }
        if heartbeat.flushing {
            match self.inner.poll_flush_unpin(cx) {
                Poll::Pending => return Ok(()),
                Poll::Ready(result) => result.map_err(io::Error::other)?,
            }
            heartbeat.flushing = false;
            heartbeat
                .deadline
                .as_mut()
                .reset(Instant::now() + heartbeat.interval);
            // Register the I/O task for the next idle heartbeat.
            let _ = heartbeat.deadline.as_mut().poll(cx);
        }
        Ok(())
    }
}

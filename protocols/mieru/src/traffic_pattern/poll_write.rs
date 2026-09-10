use alloc::{boxed::Box, vec::Vec};
use core::{
    future::Future,
    pin::Pin,
    task::{ready, Context, Poll},
    time::Duration,
};

use rand::Rng;
use tokio::{
    io::{self, AsyncWrite},
    time::Sleep,
};

use super::TcpFragmentPattern;

/// Preserves an encrypted segment's fragment and sleep boundaries across
/// `AsyncWrite` polls. One state belongs to one plain stream direction.
pub(crate) struct FragmentedWriteState {
    pattern: TcpFragmentPattern,
    wire: Vec<u8>,
    position: usize,
    fragment_end: usize,
    plain_len: usize,
    sleep: Option<Pin<Box<Sleep>>>,
}

impl FragmentedWriteState {
    pub(crate) fn new(pattern: TcpFragmentPattern) -> Self {
        Self {
            pattern,
            wire: Vec::new(),
            position: 0,
            fragment_end: 0,
            plain_len: 0,
            sleep: None,
        }
    }

    pub(crate) fn is_idle(&self) -> bool {
        self.wire.is_empty()
    }

    pub(crate) fn start(&mut self, wire: Vec<u8>, plain_len: usize) {
        debug_assert!(self.is_idle());
        self.wire = wire;
        self.position = 0;
        self.plain_len = plain_len;
        self.fragment_end = self.next_fragment_end();
    }

    pub(crate) fn poll_drain<W: AsyncWrite + Unpin>(
        &mut self,
        mut writer: Pin<&mut W>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        if self.is_idle() {
            return Poll::Ready(Ok(()));
        }

        loop {
            if let Some(sleep) = self.sleep.as_mut() {
                ready!(sleep.as_mut().poll(cx));
                self.sleep = None;
                if self.position == self.wire.len() {
                    return Poll::Ready(Ok(()));
                }
                self.fragment_end = self.next_fragment_end();
            }

            while self.position < self.fragment_end {
                match writer
                    .as_mut()
                    .poll_write(cx, &self.wire[self.position..self.fragment_end])
                {
                    Poll::Ready(Ok(0)) => {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::WriteZero,
                            "mieru write zero",
                        )));
                    }
                    Poll::Ready(Ok(written)) => self.position += written,
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                    Poll::Pending => return Poll::Pending,
                }
            }

            if !self.pattern.enable {
                return Poll::Ready(Ok(()));
            }
            if self.pattern.max_sleep_ms == 0 {
                if self.position == self.wire.len() {
                    return Poll::Ready(Ok(()));
                }
                self.fragment_end = self.next_fragment_end();
                continue;
            }

            let sleep_ms = rand::thread_rng().gen_range(0..=self.pattern.max_sleep_ms);
            self.sleep = Some(Box::pin(tokio::time::sleep(Duration::from_millis(
                sleep_ms.into(),
            ))));
        }
    }

    pub(crate) fn finish(&mut self) -> usize {
        let plain_len = self.plain_len;
        self.wire.clear();
        self.position = 0;
        self.fragment_end = 0;
        self.plain_len = 0;
        self.sleep = None;
        plain_len
    }

    /// Drains the previously accepted frame before the caller accepts a new
    /// plain buffer. Cancellation leaves the old frame owned by this state.
    pub(crate) fn poll_ready_for_write<W: AsyncWrite + Unpin>(
        &mut self,
        writer: Pin<&mut W>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        ready!(self.poll_drain(writer, cx))?;
        self.finish();
        Poll::Ready(Ok(()))
    }

    pub(crate) fn poll_flush<W: AsyncWrite + Unpin>(
        &mut self,
        mut writer: Pin<&mut W>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        ready!(self.poll_drain(writer.as_mut(), cx))?;
        self.finish();
        writer.poll_flush(cx)
    }

    pub(crate) fn poll_shutdown<W: AsyncWrite + Unpin>(
        &mut self,
        mut writer: Pin<&mut W>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        ready!(self.poll_drain(writer.as_mut(), cx))?;
        self.finish();
        writer.poll_shutdown(cx)
    }

    fn next_fragment_end(&self) -> usize {
        if !self.pattern.enable || self.wire.is_empty() {
            return self.wire.len();
        }
        let min_len = (self.wire.len() as f64).sqrt() as usize + 1;
        let max_len = min_len.max(self.wire.len() / 2);
        self.position
            .saturating_add(rand::thread_rng().gen_range(min_len..=max_len))
            .min(self.wire.len())
    }
}

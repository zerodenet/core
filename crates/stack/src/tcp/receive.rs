use std::collections::VecDeque;
use std::sync::Mutex;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use tokio::io::ReadBuf;

pub(super) struct TcpReceiveBuffer {
    capacity: usize,
    state: Mutex<ReceiveState>,
}

#[derive(Default)]
struct ReceiveState {
    chunks: VecDeque<Vec<u8>>,
    front_offset: usize,
    buffered: usize,
    closed: bool,
    reader_waker: Option<Waker>,
    released_since_update: usize,
    last_rejection_report: Option<Instant>,
    suppressed_rejections: u64,
}

pub(super) struct ReceiveRejection {
    pub buffered: usize,
    pub available: usize,
    pub closed: bool,
    pub suppressed: u64,
}

impl TcpReceiveBuffer {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.min(u16::MAX as usize),
            state: Mutex::new(ReceiveState::default()),
        }
    }

    pub(super) fn push(&self, payload: &[u8]) -> bool {
        let mut state = self.state.lock().expect("TCP receive buffer lock poisoned");
        if state.closed || payload.len() > self.capacity.saturating_sub(state.buffered) {
            return false;
        }
        state.buffered += payload.len();
        state.chunks.push_back(payload.to_vec());
        let waker = state.reader_waker.take();
        drop(state);
        if let Some(waker) = waker {
            waker.wake();
        }
        true
    }

    pub(super) fn close(&self) {
        let mut state = self.state.lock().expect("TCP receive buffer lock poisoned");
        state.closed = true;
        let waker = state.reader_waker.take();
        drop(state);
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    pub(super) fn window(&self) -> u16 {
        let state = self.state.lock().expect("TCP receive buffer lock poisoned");
        self.capacity
            .saturating_sub(state.buffered)
            .min(u16::MAX as usize) as u16
    }

    /// Report the first rejection and at most one per 30 seconds until reads
    /// resume. Keep connection identity at the caller; no payload is logged.
    pub(super) fn rejection_report(&self, now: Instant) -> Option<ReceiveRejection> {
        let mut state = self.state.lock().expect("TCP receive buffer lock poisoned");
        if state
            .last_rejection_report
            .is_some_and(|last| now.duration_since(last) < Duration::from_secs(30))
        {
            state.suppressed_rejections = state.suppressed_rejections.saturating_add(1);
            return None;
        }
        state.last_rejection_report = Some(now);
        let suppressed = std::mem::take(&mut state.suppressed_rejections);
        Some(ReceiveRejection {
            buffered: state.buffered,
            available: self.capacity.saturating_sub(state.buffered),
            closed: state.closed,
            suppressed,
        })
    }

    /// Report a reopened zero window or accumulated release of at least one
    /// segment (bounded by half the buffer). A tiny first read must not leave
    /// the peer waiting for probes after later reads free substantial capacity.
    pub(super) fn poll_read(
        &self,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
        segment_size: u16,
    ) -> (Poll<()>, bool) {
        let mut state = self.state.lock().expect("TCP receive buffer lock poisoned");
        let window_before = self.capacity.saturating_sub(state.buffered);
        let target = buf.remaining();
        let mut copied = 0;

        while copied < target {
            let Some(front) = state.chunks.pop_front() else {
                break;
            };
            let available = &front[state.front_offset..];
            let count = available.len().min(target - copied);
            buf.put_slice(&available[..count]);
            copied += count;
            state.buffered -= count;

            if count < available.len() {
                state.front_offset += count;
                state.chunks.push_front(front);
                break;
            }
            state.front_offset = 0;
        }

        state.released_since_update = state.released_since_update.saturating_add(copied);
        let threshold = usize::from(segment_size.max(1)).min((self.capacity / 2).max(1));
        let update_window =
            copied > 0 && (window_before == 0 || state.released_since_update >= threshold);
        if update_window {
            state.released_since_update = 0;
        }
        if copied > 0 {
            state.last_rejection_report = None;
            state.suppressed_rejections = 0;
        }
        if copied > 0 || state.closed {
            return (Poll::Ready(()), update_window);
        }

        state.reader_waker = Some(cx.waker().clone());
        (Poll::Pending, false)
    }
}

#[cfg(test)]
#[path = "../../tests/tcp/receive_buffer.rs"]
mod tests;

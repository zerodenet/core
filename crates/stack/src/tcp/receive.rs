use std::collections::VecDeque;
use std::sync::Mutex;
use std::task::{Context, Poll, Waker};

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

    /// Read buffered bytes and report whether a zero receive window reopened.
    pub(super) fn poll_read(
        &self,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
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

        let reopened = window_before == 0 && self.capacity.saturating_sub(state.buffered) > 0;
        if copied > 0 || state.closed {
            return (Poll::Ready(()), reopened);
        }

        state.reader_waker = Some(cx.waker().clone());
        (Poll::Pending, false)
    }
}

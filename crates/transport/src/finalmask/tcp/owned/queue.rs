//! The same two finite mask queues, polled by their actual enclosing driver.
use super::memory::Memory;
use super::*;
use futures_util::task::AtomicWaker;
use std::{
    future::poll_fn,
    sync::Mutex,
    task::{Context, Poll},
};
pub(super) struct Frame {
    pub(super) bytes: Vec<u8>,
    pub(super) _credit: CarrierBufferFrameCredit,
}
struct Ring {
    rows: [Option<Frame>; 16],
    head: usize,
    count: usize,
}
impl Ring {
    fn new() -> Self {
        Self {
            rows: std::array::from_fn(|_| None),
            head: 0,
            count: 0,
        }
    }
    fn push(&mut self, frame: Frame) {
        let index = (self.head + self.count) % 16;
        assert!(self.rows[index].is_none());
        self.rows[index] = Some(frame);
        self.count += 1;
    }
    fn pop(&mut self) -> Option<Frame> {
        if self.count == 0 {
            return None;
        }
        let frame = self.rows[self.head].take();
        self.head = (self.head + 1) % 16;
        self.count -= 1;
        frame
    }
}
struct State {
    input: Ring,
    output: Ring,
    read_eof: bool,
    write_closed: bool,
    write_finished: bool,
    accepted: u64,
    committed: u64,
    failure: Option<PacketMetadataArc<io::Error>>,
}
pub(super) struct Shared {
    state: Mutex<State>,
    read: AtomicWaker,
    write: AtomicWaker,
    input_space: AtomicWaker,
    output_ready: AtomicWaker,
    pub(super) memory: Memory,
}
impl Shared {
    pub(super) fn new(memory: Memory) -> Self {
        Self {
            state: Mutex::new(State {
                input: Ring::new(),
                output: Ring::new(),
                read_eof: false,
                write_closed: false,
                write_finished: false,
                accepted: 0,
                committed: 0,
                failure: None,
            }),
            read: AtomicWaker::new(),
            write: AtomicWaker::new(),
            input_space: AtomicWaker::new(),
            output_ready: AtomicWaker::new(),
            memory,
        }
    }
    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    pub(super) async fn publish(&self, frame: Frame) -> io::Result<()> {
        let mut frame = Some(frame);
        poll_fn(|cx| {
            self.input_space.register(cx.waker());
            let mut state = self.state();
            if state.input.count >= self.memory.queue_limit {
                return Poll::Pending;
            }
            state.input.push(frame.take().expect("same decoded frame"));
            drop(state);
            self.read.wake();
            Poll::Ready(Ok(()))
        })
        .await
    }
    pub(super) async fn next_output(&self) -> Option<Frame> {
        poll_fn(|cx| {
            self.output_ready.register(cx.waker());
            let mut state = self.state();
            let frame = state.output.pop();
            let closed = state.write_closed;
            drop(state);
            if frame.is_some() {
                self.write.wake();
                Poll::Ready(frame)
            } else if closed {
                Poll::Ready(None)
            } else {
                Poll::Pending
            }
        })
        .await
    }
    pub(super) fn poll_input(&self, cx: &mut Context<'_>) -> Poll<io::Result<Option<Frame>>> {
        self.read.register(cx.waker());
        let mut state = self.state();
        let frame = state.input.pop();
        let eof = state.read_eof;
        let failure = state.failure.clone();
        drop(state);
        if frame.is_some() {
            self.input_space.wake();
            return Poll::Ready(Ok(frame));
        }
        if let Some(failure) = failure {
            return Poll::Ready(Err(shared_error(failure, false)));
        }
        if eof {
            Poll::Ready(Ok(None))
        } else {
            Poll::Pending
        }
    }
    pub(super) fn poll_output_slot(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.write.register(cx.waker());
        let state = self.state();
        let failure = state.failure.clone();
        let closed = state.write_closed;
        let available = state.output.count < self.memory.queue_limit;
        drop(state);
        if let Some(failure) = failure {
            return Poll::Ready(Err(shared_error(failure, true)));
        }
        if closed {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        }
        if available {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
    pub(super) fn push_output(&self, frame: Frame) {
        let mut state = self.state();
        state.accepted = state.accepted.wrapping_add(frame.bytes.len() as u64);
        state.output.push(frame);
        drop(state);
        self.output_ready.wake();
    }
    pub(super) fn commit(&self, bytes: usize) {
        {
            let mut state = self.state();
            state.committed = state.committed.wrapping_add(bytes as u64);
        }
        self.write.wake();
    }
    pub(super) fn finish_read(&self) {
        self.state().read_eof = true;
        self.read.wake();
    }
    pub(super) fn finish_write(&self) {
        self.state().write_finished = true;
        self.write.wake();
    }
    pub(super) fn close_output(&self) {
        self.state().write_closed = true;
        self.output_ready.wake();
    }
    pub(super) fn poll_progress(
        &self,
        cx: &mut Context<'_>,
        shutdown: bool,
    ) -> Poll<io::Result<()>> {
        self.write.register(cx.waker());
        let state = self.state();
        let failure = state.failure.clone();
        let ready = state.committed >= state.accepted && (!shutdown || state.write_finished);
        drop(state);
        if let Some(failure) = failure {
            Poll::Ready(Err(shared_error(failure, true)))
        } else if ready {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
    pub(super) fn fail(&self, error: io::Error) {
        let original = self.memory.failure(error);
        let mut state = self.state();
        if state.failure.is_none() {
            state.failure = Some(original);
            drop(state);
        } else {
            drop(state);
            drop(original);
        }
        self.read.wake();
        self.write.wake();
    }
}
struct SharedFailure(PacketMetadataArc<io::Error>);
impl std::fmt::Debug for SharedFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&*self.0, f)
    }
}
impl std::fmt::Display for SharedFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&*self.0, f)
    }
}
impl std::error::Error for SharedFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&*self.0)
    }
}
fn shared_error(original: PacketMetadataArc<io::Error>, writing: bool) -> io::Error {
    // Preserve the original Progress writer's Other classification; the same
    // actual native source remains reachable instead of becoming a String.
    let kind = if writing {
        io::ErrorKind::Other
    } else {
        original.kind()
    };
    io::Error::new(kind, SharedFailure(original))
}

//! The existing eight-frame body FIFO, with concrete paid storage and no task.
use super::*;
use bytes::Bytes;
use std::sync::Mutex;
use zero_traits::packet_metadata::{PacketMetadataCredit, PacketMetadataLayout};
const CAP: usize = 8;
type Frame = io::Result<Bytes>;
struct Queue {
    state: Mutex<State>,
    reader: futures_util::task::AtomicWaker,
    writer: futures_util::task::AtomicWaker,
}
struct State {
    rows: Vec<Option<Frame>>,
    head: usize,
    len: usize,
    tx: bool,
    rx: bool,
    _memory: PacketMetadataCredit,
}
pub(super) struct Sender(PacketMetadataArc<Queue>);
pub(in crate::split_http) struct Receiver(PacketMetadataArc<Queue>);
pub(super) fn channel(memory: &PacketDriverMetadataView) -> io::Result<(Sender, Receiver)> {
    let header = PacketMetadataArc::<Queue>::layout().map_err(io::Error::other)?;
    let rows = PacketMetadataLayout::array::<Option<Frame>>(CAP).map_err(io::Error::other)?;
    let mut paid = memory
        .reserve(header.checked_add(rows).map_err(io::Error::other)?)
        .map_err(io::Error::other)?;
    let header = paid.split(header).map_err(io::Error::other)?;
    let mut rows = Vec::new();
    rows.try_reserve_exact(CAP).map_err(io::Error::other)?;
    rows.resize_with(CAP, || None);
    let queue = PacketMetadataArc::from_prepaid(
        Queue {
            state: Mutex::new(State {
                rows,
                head: 0,
                len: 0,
                tx: true,
                rx: true,
                _memory: paid,
            }),
            reader: Default::default(),
            writer: Default::default(),
        },
        header,
    )
    .map_err(|(source, _, _)| io::Error::other(source))?;
    Ok((Sender(queue.clone()), Receiver(queue)))
}
impl Sender {
    pub async fn send(&mut self, frame: Frame) -> io::Result<()> {
        let mut frame = Some(frame);
        std::future::poll_fn(|cx| {
            self.0.writer.register(cx.waker());
            let mut state = self
                .0
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !state.rx {
                return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
            }
            if state.len == CAP {
                return Poll::Pending;
            }
            let tail = (state.head + state.len) % CAP;
            assert!(state.rows[tail].is_none());
            state.rows[tail] = frame.take();
            state.len += 1;
            drop(state);
            self.0.reader.wake();
            Poll::Ready(Ok(()))
        })
        .await
    }
}
impl Receiver {
    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<Frame>> {
        self.0.reader.register(cx.waker());
        let mut state = self
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.len == 0 {
            return if state.tx {
                Poll::Pending
            } else {
                Poll::Ready(None)
            };
        }
        let head = state.head;
        let frame = state.rows[head].take().expect("same accepted body frame");
        state.head = (head + 1) % CAP;
        state.len -= 1;
        drop(state);
        self.0.writer.wake();
        Poll::Ready(Some(frame))
    }
    pub fn is_closed(&self) -> bool {
        !self
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .tx
    }
    pub fn is_empty(&self) -> bool {
        self.0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len
            == 0
    }
}
impl Drop for Sender {
    fn drop(&mut self) {
        self.0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .tx = false;
        self.0.reader.wake();
    }
}
impl Drop for Receiver {
    fn drop(&mut self) {
        self.0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .rx = false;
        loop {
            let frame = {
                let mut state = self
                    .0
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if state.len == 0 {
                    None
                } else {
                    let head = state.head;
                    let frame = state.rows[head].take();
                    state.head = (head + 1) % CAP;
                    state.len -= 1;
                    frame
                }
            };
            if frame.is_none() {
                break;
            }
            drop(frame);
        }
        self.0.writer.wake();
    }
}

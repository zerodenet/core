//! The existing idle browser queue; hosted storage belongs to the actual Root.
use super::IdleConnection;
use std::{
    io,
    ops::Deref,
    sync::{Arc, Mutex},
};
use tokio::sync::Notify;
use zero_platform_tokio::contract::component::PacketDriverMetadataView;
use zero_traits::packet_metadata::PacketMetadataArc;
use zero_traits::packet_metadata::{PacketMetadataCredit, PacketMetadataLayout};

struct State {
    rows: Vec<Option<IdleConnection>>,
    head: usize,
    len: usize,
    closed: bool,
    // The actual backing above is freed before its prepaid credit.
    _storage: Option<PacketMetadataCredit>,
}
pub(super) struct Queue {
    state: Mutex<State>,
    changed: Notify,
}
#[derive(Clone)]
pub(super) enum IdleQueue {
    Library(Arc<Queue>),
    Hosted(PacketMetadataArc<Queue>),
}
impl Deref for IdleQueue {
    type Target = Queue;
    fn deref(&self) -> &Queue {
        match self {
            Self::Library(queue) => queue,
            Self::Hosted(queue) => queue,
        }
    }
}
impl Queue {
    fn prepare(capacity: usize, storage: Option<PacketMetadataCredit>) -> io::Result<Self> {
        if capacity == 0 {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        // The concrete requested array is paid before allocation on the hosted
        // path. No reserve/realloc can silently enlarge its reported capacity.
        let slots = Box::<[Option<IdleConnection>]>::new_uninit_slice(capacity);
        let pointer = Box::into_raw(slots) as *mut Option<IdleConnection>;
        // SAFETY: identical array layout/alignment, initially no live elements.
        let mut rows = unsafe { Vec::from_raw_parts(pointer, 0, capacity) };
        rows.resize_with(capacity, || None);
        Ok(Self {
            state: Mutex::new(State {
                rows,
                head: 0,
                len: 0,
                closed: false,
                _storage: storage,
            }),
            changed: Notify::new(),
        })
    }
}
impl IdleQueue {
    pub(super) fn library(capacity: usize) -> io::Result<Self> {
        Ok(Self::Library(Arc::new(Queue::prepare(capacity, None)?)))
    }
    pub(super) fn hosted(capacity: usize, memory: &PacketDriverMetadataView) -> io::Result<Self> {
        let rows = PacketMetadataLayout::array::<Option<IdleConnection>>(capacity)
            .map_err(io::Error::other)?;
        let layout = PacketMetadataArc::<Queue>::layout().map_err(io::Error::other)?;
        let mut paid = memory
            .reserve(layout.checked_add(rows).map_err(io::Error::other)?)
            .map_err(io::Error::other)?;
        let header = paid.split(layout).map_err(io::Error::other)?;
        let queue = Queue::prepare(capacity, Some(paid))?;
        let queue = PacketMetadataArc::from_prepaid(queue, header)
            .map_err(|(source, _, _)| io::Error::other(source))?;
        Ok(Self::Hosted(queue))
    }
    pub(super) async fn send(&self, connection: IdleConnection) -> io::Result<()> {
        let mut original = Some(connection);
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let mut state = self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if state.closed {
                    return Err(io::ErrorKind::BrokenPipe.into());
                }
                let capacity = state.rows.len();
                if state.len < capacity {
                    let tail = (state.head + state.len) % capacity;
                    assert!(state.rows[tail].is_none());
                    state.rows[tail] = original.take();
                    state.len += 1;
                    drop(state);
                    self.changed.notify_waiters();
                    return Ok(());
                }
            }
            changed.await;
        }
    }
    pub(super) async fn recv(&self) -> Option<IdleConnection> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let mut state = self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if state.len != 0 {
                    let head = state.head;
                    let connection = state.rows[head].take();
                    state.head = (head + 1) % state.rows.len();
                    state.len -= 1;
                    drop(state);
                    self.changed.notify_waiters();
                    return connection;
                }
                if state.closed {
                    return None;
                }
            }
            changed.await;
        }
    }
    #[cfg(test)]
    pub(super) fn original_for_test(&self) -> Option<super::BrowserOriginal> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .rows
            .iter()
            .flatten()
            .find_map(|connection| {
                connection
                    .original
                    .as_ref()
                    .map(|original| original.0.clone())
            })
    }
    pub(super) fn close(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .closed = true;
        // Returning an original can synchronously wake its real driver. Never
        // drop a socket/credit while the actual queue is locked.
        loop {
            let connection = {
                let mut state = self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if state.len == 0 {
                    break;
                }
                let head = state.head;
                let connection = state.rows[head].take();
                state.head = (head + 1) % state.rows.len();
                state.len -= 1;
                connection
            };
            drop(connection);
        }
        self.changed.notify_waiters();
    }
}

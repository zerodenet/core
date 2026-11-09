//! Concrete XMUX session/request work in its existing physical HTTP driver.
//! No Tokio task or independent receiver is created per logical session/POST.
use super::Work;
use std::{
    future::Future,
    io,
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll},
};
use tokio::sync::Notify;
use zero_platform_tokio::contract::component::PacketDriverMetadataView;
use zero_traits::packet_metadata::{PacketMetadataArc, PacketMetadataCredit, PacketMetadataLayout};
pub(super) struct Ledger {
    memory: PacketDriverMetadataView,
    limit: usize,
    state: Mutex<State>,
    pub changed: Notify,
}
struct State {
    next: u64,
    count: usize,
    sealed: bool,
    head: Option<PacketMetadataArc<Node>>,
}
struct Node {
    ordinal: u64,
    next: Mutex<Option<PacketMetadataArc<Node>>>,
    work: Mutex<Option<Work>>,
}
impl Ledger {
    pub fn new(
        memory: PacketDriverMetadataView,
        limit: usize,
    ) -> io::Result<PacketMetadataArc<Self>> {
        if limit == 0 {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let header = memory
            .reserve(PacketMetadataArc::<Self>::layout().map_err(io::Error::other)?)
            .map_err(io::Error::other)?;
        PacketMetadataArc::from_prepaid(
            Self {
                memory,
                limit,
                state: Mutex::new(State {
                    next: 0,
                    count: 0,
                    sealed: false,
                    head: None,
                }),
                changed: Notify::new(),
            },
            header,
        )
        .map_err(|(source, _, _)| io::Error::other(source))
    }
    /// Only the concrete Session and Request constructors can create Work.
    /// All storage is reserved before the node/future allocation is published.
    pub fn push(&self, work: Work) -> io::Result<()> {
        let header = self
            .memory
            .reserve(PacketMetadataArc::<Node>::layout().map_err(io::Error::other)?)
            .map_err(io::Error::other)?;
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.sealed || state.count >= self.limit {
            drop(state);
            drop(header);
            drop(work);
            return Err(io::ErrorKind::WouldBlock.into());
        }
        let ordinal = state
            .next
            .checked_add(1)
            .ok_or_else(|| io::Error::other("actual XMUX work identity exhausted"))?;
        state.next = ordinal;
        state.count += 1;
        drop(state);
        let reservation = Reservation {
            ledger: self,
            active: true,
        };
        let node = PacketMetadataArc::from_prepaid(
            Node {
                ordinal,
                next: Mutex::new(None),
                work: Mutex::new(Some(work)),
            },
            header,
        )
        .unwrap_or_else(|_| panic!("XMUX node layout prepaid"));
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.sealed {
            drop(state);
            drop(node);
            return Err(io::ErrorKind::BrokenPipe.into());
        }
        *node
            .next
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = state.head.take();
        state.head = Some(node);
        drop(state);
        reservation.publish();
        self.changed.notify_one();
        Ok(())
    }
    pub fn poll(&self, cx: &mut Context<'_>) -> Poll<()> {
        let mut cursor = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .head
            .clone();
        while let Some(node) = cursor {
            cursor = node
                .next
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            // No owner/record lock crosses actual future.poll or its destructor.
            let work = node
                .work
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            if let Some(mut work) = work {
                if work.poll(cx).is_ready() {
                    let removed = self.remove(node.ordinal);
                    drop(work);
                    drop(removed);
                } else {
                    *node
                        .work
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(work);
                }
            }
        }
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.sealed && state.count == 0 {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
    fn remove(&self, ordinal: u64) -> Option<PacketMetadataArc<Node>> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut previous: Option<PacketMetadataArc<Node>> = None;
        let mut current = state.head.clone();
        while let Some(node) = current {
            let next = node
                .next
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            if node.ordinal == ordinal {
                let removed = match previous {
                    Some(previous) => std::mem::replace(
                        &mut *previous
                            .next
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner),
                        next,
                    ),
                    None => std::mem::replace(&mut state.head, next),
                };
                state.count -= 1;
                drop(state);
                return removed;
            }
            previous = Some(node);
            current = next;
        }
        None
    }
    /// Physical driver teardown returns/drops every original concrete work item
    /// outside all owner gates. This is not NativeClosed or a task ACK.
    pub fn cancel_all(&self) {
        let retired = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.sealed = true;
            let mut cursor = state.head.clone();
            let mut published = 0;
            while let Some(node) = cursor {
                published += 1;
                cursor = node
                    .next
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone();
            }
            state.count -= published;
            state.head.take()
        };
        let mut cursor = retired;
        while let Some(node) = cursor {
            cursor = node
                .next
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            drop(node);
        }
        self.changed.notify_waiters();
    }
}
/// One actual concrete future, paid before Box::pin; the box is destroyed before
/// its metadata ticket. This type is private and accepts no public callback SPI.
pub(super) struct PaidFuture {
    inner: Pin<Box<dyn Future<Output = ()> + Send>>,
    _memory: PacketMetadataCredit,
    // Original work/future allocation is destroyed before publishing return.
    _returned: Option<super::logical::WorkReturn>,
}
impl PaidFuture {
    pub(super) fn prepare_returning<F: Future<Output = ()> + Send + 'static>(
        future: F,
        memory: &PacketDriverMetadataView,
        returned: Option<super::logical::WorkReturn>,
    ) -> io::Result<Self> {
        let credit = memory
            .reserve(PacketMetadataLayout::boxed::<F>())
            .map_err(io::Error::other)?;
        Ok(Self {
            inner: Box::pin(future),
            _memory: credit,
            _returned: returned,
        })
    }
    pub(super) fn poll(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        self.inner.as_mut().poll(cx)
    }
}

struct Reservation<'a> {
    ledger: &'a Ledger,
    active: bool,
}
impl Reservation<'_> {
    fn publish(mut self) {
        self.active = false;
    }
}
impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        if self.active {
            let mut state = self
                .ledger
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.count -= 1;
            drop(state);
            self.ledger.changed.notify_one();
        }
    }
}

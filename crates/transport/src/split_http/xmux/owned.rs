//! Concrete owned XMUX work shared with the actual physical HTTP driver.
//! These private records cannot accept public arbitrary execution callbacks.
mod carrier;
pub use carrier::{OwnedXhttp3, PendingXhttp3, PreparedXhttp3};
mod ledger;
mod logical;
pub use logical::OwnedLogicalReturn;
pub(in crate::split_http) use logical::WorkReturn;
pub(in crate::split_http) mod pipe;
pub(in crate::split_http) mod request;
mod session;
pub(in crate::split_http) use session::PreparedLogical;
mod sources;
pub(in crate::split_http) use sources::RequestCauses;
mod tcp;
use ledger::{Ledger, PaidFuture};
use std::{
    io,
    sync::Arc,
    task::{Context, Poll},
};
pub(super) use tcp::Original as TcpOriginal;
pub use tcp::{OwnedXhttpTcp, PendingXhttpTcp, PreparedXhttpTcp, TcpVersion};
use zero_platform_tokio::{
    contract::{component::PacketDriverMetadataView, CancellationToken},
    controlled_carrier::CarrierBufferBudget,
};
use zero_traits::packet_metadata::PacketMetadataArc;
enum Work {
    Session(PaidFuture),
    Request(PaidFuture),
    Http2Delegate(PaidFuture),
}
impl Work {
    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        match self {
            Self::Session(work) | Self::Request(work) | Self::Http2Delegate(work) => work.poll(cx),
        }
    }
}
/// The original driver owns this ledger; senders do not own its physical IO or
/// its Future. Component shutdown seals it before returning original task ACKs.
pub(crate) struct Runtime {
    ledger: PacketMetadataArc<Ledger>,
    memory: PacketDriverMetadataView,
    book: CarrierBufferBudget,
    stopped: CancellationToken,
    pool: std::sync::Mutex<Option<std::sync::Weak<dyn OriginalPoolIdle>>>,
}
// Erase only the existing pool's passive maintenance reference. The same Arc
// allocation is unsized, with no wrapper allocation or new pool/executor. This
// cuts the auto-trait expansion through pool -> original -> Runtime -> pool.
trait OriginalPoolIdle: Send + Sync {
    fn reap(self: Arc<Self>);
}
impl OriginalPoolIdle for super::Inner {
    fn reap(self: Arc<Self>) {
        super::XhttpClientPool::reap_inner(&self);
    }
}
impl Runtime {
    pub fn prepare(
        book: CarrierBufferBudget,
        memory: PacketDriverMetadataView,
        actual_limit: usize,
    ) -> io::Result<PacketMetadataArc<Self>> {
        let header = memory
            .reserve(PacketMetadataArc::<Self>::layout().map_err(io::Error::other)?)
            .map_err(io::Error::other)?;
        let ledger = Ledger::new(memory.clone(), actual_limit)?;
        PacketMetadataArc::from_prepaid(
            Self {
                ledger,
                memory,
                book,
                stopped: CancellationToken::new(),
                pool: std::sync::Mutex::new(None),
            },
            header,
        )
        .map_err(|(source, _, _)| io::Error::other(source))
    }
    pub(super) fn bind_pool(&self, pool: &Arc<super::Inner>) {
        let original: Arc<dyn OriginalPoolIdle> = pool.clone();
        let original = Arc::downgrade(&original);
        let mut actual = self
            .pool
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(actual) = &*actual {
            assert!(actual.ptr_eq(&original));
        } else {
            *actual = Some(original);
        }
    }
    pub async fn run(&self) {
        tokio::select! { biased;
            _=self.stopped.cancelled()=>self.ledger.cancel_all(),
            _=self.run_work()=>{},
        }
    }
    // TCP retirement keeps polling actual HTTP work until its same wire
    // driver flushes body EOF and returns. Its own loop handles stop requests.
    async fn run_work(&self) {
        let mut idle = tokio::time::interval_at(
            tokio::time::Instant::now() + std::time::Duration::from_secs(15),
            std::time::Duration::from_secs(15),
        );
        idle.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            let changed = self.ledger.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let progress = std::future::poll_fn(|cx| self.ledger.poll(cx));
            tokio::select! {biased;
                _=progress=>return,
                _=&mut changed=>{},
                _=idle.tick()=>{let pool={self.pool.lock().unwrap_or_else(std::sync::PoisonError::into_inner).as_ref().and_then(std::sync::Weak::upgrade)};if let Some(pool)=pool{pool.reap();}},
            }
        }
    }
    fn request_tcp_return(&self) {
        self.stopped.cancel();
        self.ledger.request_logical_return();
        self.ledger.changed.notify_waiters();
    }
    pub fn stop(&self) {
        self.stopped.cancel();
        // Also breaks queued request/session->Runtime links if the sole real
        // driver has unwound. Actual in-poll work still belongs to that task
        // until its genuine join; no native/task/ACK completion is fabricated.
        self.ledger.cancel_all();
    }
    // An H1 exchange may close its physical connection while this exact
    // task still owns logical work using other originals. Keep polling that
    // work here; a normal HTTP EOF is not a logical cancellation request.
    async fn drain(&self) {
        loop {
            let changed = self.ledger.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let progress = std::future::poll_fn(|cx| {
                // A work item temporarily in poll may have missed the external
                // request. Replay only the request after it returns to this ledger.
                if self.stopped.is_cancelled() {
                    self.ledger.request_logical_return();
                }
                let _ = self.ledger.poll(cx);
                if self.ledger.is_empty() {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            });
            tokio::select! { biased;
                _=progress=>return,
                _=&mut changed=>{},
            }
        }
    }
    pub(crate) fn buffer_book(&self) -> &CarrierBufferBudget {
        &self.book
    }
}

#[cfg(test)]
#[path = "../../../tests/split_http/owned_drain.rs"]
mod tests;

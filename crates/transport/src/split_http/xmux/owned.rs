//! Concrete owned XMUX work shared with the actual physical HTTP driver.
//! These private records cannot accept public arbitrary execution callbacks.
mod carrier;
pub use carrier::{OwnedXhttp3, PendingXhttp3, PreparedXhttp3};
mod ledger;
mod logical;
pub use logical::OwnedLogicalReturn;
pub(in crate::split_http) mod pipe;
pub(in crate::split_http) mod request;
mod session;
mod sources;
use ledger::{Ledger, PaidFuture};
use std::{
    io,
    sync::Arc,
    task::{Context, Poll},
};
use zero_platform_tokio::{
    contract::{component::PacketDriverMetadataView, CancellationToken},
    controlled_carrier::CarrierBufferBudget,
};
use zero_traits::packet_metadata::PacketMetadataArc;
pub(super) enum Work {
    Session(PaidFuture),
    Request(PaidFuture),
}
impl Work {
    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        match self {
            Self::Session(work) | Self::Request(work) => work.poll(cx),
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
    pool: std::sync::Mutex<Option<std::sync::Weak<super::Inner>>>,
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
        let mut actual = self
            .pool
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(actual) = &*actual {
            assert!(actual.ptr_eq(&Arc::downgrade(pool)));
        } else {
            *actual = Some(Arc::downgrade(pool));
        }
    }
    pub async fn run(&self) {
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
                _=self.stopped.cancelled()=>{self.ledger.cancel_all();return;},
                _=progress=>return,
                _=&mut changed=>{},
                _=idle.tick()=>{let pool={self.pool.lock().unwrap_or_else(std::sync::PoisonError::into_inner).as_ref().and_then(std::sync::Weak::upgrade)};if let Some(pool)=pool{super::XhttpClientPool::reap_inner(&pool);}},
            }
        }
    }
    pub fn stop(&self) {
        self.stopped.cancel();
        // Also breaks queued request/session->Runtime links if the sole real
        // driver has unwound. Actual in-poll work still belongs to that task
        // until its genuine join; no native/task/ACK completion is fabricated.
        self.ledger.cancel_all();
    }
    pub(crate) fn buffer_book(&self) -> &CarrierBufferBudget {
        &self.book
    }
}

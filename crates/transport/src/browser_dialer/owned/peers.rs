//! Same paid peer rows; no copied census or secondary connection owner.
use super::*;
use std::sync::Mutex;
use tokio::sync::Notify;
use zero_platform_tokio::contract::component::PacketMetadataVec;
use zero_traits::packet_metadata::PacketMetadataWeak;
pub(in crate::browser_dialer) struct Peer {
    pub(in crate::browser_dialer) original: AcceptedTcpClaim,
    pub(in crate::browser_dialer) task: OnceLock<(TaskId, TaskJoinWitness)>,
    pub(in crate::browser_dialer) closed: OnceLock<AcceptedTcpClosed>,
    pub(in crate::browser_dialer) cancel: CancellationToken,
    pub(in crate::browser_dialer) parent: PacketMetadataWeak<Peers>,
    pub(in crate::browser_dialer) changed: Notify,
    #[cfg(test)]
    pub(in crate::browser_dialer) read_pending: std::sync::atomic::AtomicBool,
}
pub(in crate::browser_dialer) struct Peers {
    rows: Mutex<PacketMetadataVec<PacketMetadataArc<Peer>>>,
    memory: PacketDriverMetadataView,
    pub(super) receiver: OnceLock<AcceptedTcpTaskReceiver>,
    pub(in crate::browser_dialer) changed: Notify,
    limit: usize,
}
pub(super) struct Prepared {
    header: zero_traits::packet_metadata::PacketMetadataCredit,
    growth: Option<zero_traits::packet_metadata::PacketMetadataCredit>,
    memory: crate::browser_dialer::memory::Memory,
}
impl Peers {
    #[cfg(test)]
    pub(in crate::browser_dialer) fn pending_header_for_test(
        &self,
    ) -> Option<crate::browser_dialer::BrowserOriginal> {
        self.rows
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .find(|peer| peer.read_pending.load(std::sync::atomic::Ordering::Acquire))
            .cloned()
            .map(crate::browser_dialer::BrowserOriginal)
    }
    pub(super) fn new(memory: PacketDriverMetadataView, limit: usize) -> Self {
        Self {
            rows: Mutex::new(PacketMetadataVec::default()),
            memory,
            receiver: OnceLock::new(),
            changed: Notify::new(),
            limit,
        }
    }

    pub(super) async fn prepare(
        this: &PacketMetadataArc<Self>,
        factory: &crate::browser_dialer::memory::Factory,
    ) -> Result<Prepared, ChannelError> {
        use zero_traits::packet_metadata::PacketMetadataLayout;
        let header = PacketMetadataArc::<Peer>::layout()
            .map_err(|source| failure(Stage::Prepare, Reason::Capacity).with_source(source))?;
        let buffers = PacketMetadataArc::<crate::browser_dialer::memory::Buffers>::layout()
            .map_err(|source| failure(Stage::Prepare, Reason::Capacity).with_source(source))?;
        let cancel =
            PacketMetadataLayout::boxed::<tokio_util::sync::WaitForCancellationFutureOwned>();
        let growth = loop {
            let changed = this.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let growth = {
                let rows = this
                    .rows
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if rows.len() == this.limit {
                    None
                } else {
                    Some(rows.growth_layout(this.limit))
                }
            };
            if let Some(growth) = growth {
                break growth.map_err(|source| {
                    failure(Stage::Prepare, Reason::Capacity).with_source(source)
                })?;
            }
            changed.await;
        };
        let layout = header
            .checked_add(buffers)
            .and_then(|layout| layout.checked_add(cancel))
            .and_then(|layout| {
                layout.checked_add(growth.unwrap_or(PacketMetadataLayout::new(0, 0)))
            })
            .map_err(|source| failure(Stage::Prepare, Reason::Capacity).with_source(source))?;
        let mut credit = this
            .memory
            .reserve_owned(layout)
            .await
            .map_err(|source| failure(Stage::Prepare, Reason::Capacity).with_source(source))?;
        let peer_header = credit
            .split(header)
            .map_err(|source| failure(Stage::Prepare, Reason::Capacity).with_source(source))?;
        let buffer_header = credit
            .split(buffers)
            .map_err(|source| failure(Stage::Prepare, Reason::Capacity).with_source(source))?;
        let cancel_header = credit
            .split(cancel)
            .map_err(|source| failure(Stage::Prepare, Reason::Capacity).with_source(source))?;
        let memory = factory
            .connection(buffer_header, cancel_header)
            .await
            .map_err(|source| failure(Stage::Prepare, Reason::Capacity).with_source(source))?;
        Ok(Prepared {
            header: peer_header,
            growth: growth.map(|_| credit),
            memory,
        })
    }
    pub(super) fn install(
        this: &PacketMetadataArc<Self>,
        original: AcceptedTcpClaim,
        cancel: CancellationToken,
        prepared: Prepared,
    ) -> Result<
        (
            crate::browser_dialer::BrowserOriginal,
            crate::browser_dialer::memory::Memory,
        ),
        ChannelError,
    > {
        let peer = PacketMetadataArc::from_prepaid(
            Peer::new(original, PacketMetadataArc::downgrade(this), cancel),
            prepared.header,
        )
        .map_err(|(source, _, _)| failure(Stage::Prepare, Reason::Capacity).with_source(source))?;
        let original = crate::browser_dialer::BrowserOriginal(peer.clone());
        let _notifications = this.memory.notifications();
        let old = {
            let mut rows = this
                .rows
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let old = if let Some(growth) = prepared.growth {
                match rows.grow_prepaid(growth, this.limit) {
                    Ok(old) => old,
                    Err((source, credit)) => {
                        drop(rows);
                        drop(credit);
                        return Err(failure(Stage::Prepare, Reason::Capacity).with_source(source));
                    }
                }
            } else {
                None
            };
            let inserted = rows.push_reserved(peer);
            drop(rows);
            if let Err(peer) = inserted {
                drop(peer);
                drop(old);
                return Err(failure(Stage::Prepare, Reason::Capacity));
            }
            old
        };
        drop(old); // unused/old actual array credit outside concrete owner lock
        Ok((original, prepared.memory))
    }
    /// Observe only original joined receipts, then ask the retained native
    /// provider to close that exact returned entry. Never cancel an active peer.
    pub(super) fn reap(&self) -> Result<bool, ChannelError> {
        let receiver = self
            .receiver
            .get()
            .ok_or_else(|| failure(Stage::Close, Reason::Revoked))?;
        let mut previous: Option<u64> = None;
        loop {
            // A single actual row cursor, no unbudgeted snapshot allocation.
            let row = {
                let rows = self
                    .rows
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                rows.iter()
                    .filter(|peer| previous.is_none_or(|id| peer.original.id() > id))
                    .min_by_key(|peer| peer.original.id())
                    .cloned()
            };
            let Some(peer) = row else {
                break;
            };
            let original = &peer.original;
            previous = Some(original.id());
            if let Some((id, witness)) = peer.task.get() {
                if witness.try_joined().is_some() {
                    let _ = receiver.observe_joined_for_claim(original, *id);
                }
            }
            match receiver.try_close_if_joined(original)? {
                Some(closed) if closed.matches(original) => {
                    let _notifications = self.memory.notifications();
                    let old = {
                        let mut rows = self
                            .rows
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        let position = rows
                            .iter()
                            .position(|same| PacketMetadataArc::ptr_eq(same, &peer));
                        position.map(|index| rows.remove(index))
                    };
                    drop(old); // callback/credit after the actual row lock
                    let _ = peer.closed.set(closed);
                    peer.notify(); // notification follows actual row removal
                }
                _ => {}
            }
        }
        Ok(self
            .rows
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty())
    }
}

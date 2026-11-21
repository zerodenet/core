//! A related control/packet pair waits without holding half its reservation.
use super::*;
use tokio::time::{timeout_at, Instant};

impl CarrierGrants {
    /// Wait within the caller's existing deadline for two actual original
    /// credits. Only confirmed retirement wakes local capacity; cancellation
    /// and view return never refund a live original. No I/O or task is started.
    pub async fn reserve_pair(&self, deadline: Instant) -> Result<[CarrierGrant; 2], ChannelError> {
        if self.0.limit.get() < 2
            || self
                .0
                .totals
                .get()
                .is_some_and(|totals| totals.snapshot().limit.get() < 2)
        {
            return Err(error(
                self.instance(),
                Stage::Prepare,
                Reason::Capacity,
                DeliveryState::NotSubmitted,
            ));
        }
        let waiting = async {
            loop {
                let changed = self.0.capacity_changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                let available = {
                    let state = self.0.state();
                    self.0.limit.get().saturating_sub(state.slots.len()) >= 2
                };
                if !available {
                    changed.await;
                    continue;
                }
                if let Some(totals) = self.0.totals.get() {
                    totals.reconcile();
                    totals.wait_capacity_for(2).await;
                }
                // A racing caller can still win admission. Both attempts are
                // synchronous: failure returns the first unused original before
                // waiting again, never parking with half a pair.
                let pair: Result<_, ChannelError> = (|| {
                    let parent = self.reserve()?;
                    let child = self.reserve()?;
                    Ok([parent, child])
                })();
                match pair {
                    Err(source) if source.reason == Reason::Capacity => {
                        tokio::task::yield_now().await;
                    }
                    result => return result,
                }
            }
        };
        tokio::select! { biased;
            _ = self.0.cancel.cancelled() => Err(error(
                self.instance(), Stage::Prepare, Reason::Revoked, DeliveryState::NotSubmitted,
            )),
            result = timeout_at(deadline, waiting) => result.map_err(|source| error(
                self.instance(), Stage::Prepare, Reason::Timeout, DeliveryState::NotSubmitted,
            ).with_source(source))?,
        }
    }
}

//! A constant-storage request to the same actual driver's exclusive I/O guard.
use super::*;
use tokio::time::{timeout_at, Instant};
impl DirectEndpoint {
    /// The host invokes this before authorizing another policy/network epoch.
    /// Completion proves maintenance ran under the unique I/O guard, not that
    /// the peer task exited. Unknown original close outcomes remain failures.
    pub(crate) async fn refresh_sources(&self, deadline: Instant) -> Result<(), ChannelError> {
        let request = {
            let mut state = self
                .0
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !self.0.bound_mode {
                return Err(error(
                    Stage::Prepare,
                    Reason::Unsupported,
                    DeliveryState::NotSubmitted,
                ));
            }
            if state.closing || self.0.scope_cancel.is_cancelled() {
                return Err(error(
                    Stage::Prepare,
                    Reason::Revoked,
                    DeliveryState::NotSubmitted,
                ));
            }
            let request = state.refresh_requested.checked_add(1).ok_or_else(|| {
                error(
                    Stage::Prepare,
                    Reason::Capacity,
                    DeliveryState::NotSubmitted,
                )
            })?;
            state.refresh_requested = request;
            request
        };
        self.0.changed.notify_one();
        timeout_at(deadline, async {
            loop {
                let changed = self.0.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                {
                    let state = self
                        .0
                        .state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if let Some(source) = &state.failure {
                        return Err(error(Stage::Close, Reason::Io, DeliveryState::Unknown)
                            .with_source(source.clone()));
                    }
                    if state.refresh_completed >= request {
                        return Ok(());
                    }
                    if state.closing || self.0.scope_cancel.is_cancelled() {
                        return Err(error(
                            Stage::Prepare,
                            Reason::Revoked,
                            DeliveryState::NotSubmitted,
                        ));
                    }
                }
                changed.await;
            }
        })
        .await
        .map_err(|source| {
            error(Stage::Close, Reason::Timeout, DeliveryState::Unknown).with_source(source)
        })?
    }
}

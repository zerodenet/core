//! One actual Service demand resolves once; every failed socket keeps its own slot.
use super::*;
use std::sync::Arc;
use zero_core::request::Destination;
use zero_platform_tokio::{
    contract::{
        DeliveryState,
        network::{CarrierContext, PendingCarrierObserver, UpstreamTransport},
    },
    controlled_carrier::{CarrierCandidates, CarrierGrant, PreparedCarrier},
};
pub(super) struct Candidates {
    call: CarrierContext,
    state: tokio::sync::Mutex<Option<(CarrierCandidates, usize)>>,
}
impl Candidates {
    pub(super) async fn prepare(
        &self,
        upstream: &UpstreamTransport,
        endpoint: Destination,
        grant: CarrierGrant,
    ) -> Result<PreparedCarrier, ChannelError> {
        let mut state = self.state.lock().await;
        if state.is_none() {
            *state = Some((
                upstream
                    .resolve_controlled_carrier(endpoint, self.call.clone())
                    .await?,
                0,
            ));
        }
        let (candidates, next) = state.as_mut().unwrap();
        let prepared = candidates.prepare_stream(*next, grant)?;
        // Persist the exact next index before native connect's first await.
        *next += 1;
        Ok(prepared)
    }
    async fn has_next(&self) -> bool {
        self.state
            .lock()
            .await
            .as_ref()
            .is_some_and(|(candidates, next)| *next < candidates.len())
    }
}
impl Managed {
    pub(super) async fn create(
        self: &Arc<Self>,
        demand: &Demand,
        waiting: Option<PendingCarrierObserver>,
    ) -> Result<Arc<NativeClient>, ChannelError> {
        let candidates = (self.options.selected_background && demand.binding.prefix.is_empty())
            .then(|| {
                Arc::new(Candidates {
                    // This is the original whole demand's cancellation/deadline. A
                    // failed first physical Slot must not revoke later DNS candidates.
                    call: CarrierContext {
                        session: None,
                        deadline: demand.deadline,
                        cancel: demand.cancel.clone(),
                    },
                    state: tokio::sync::Mutex::new(None),
                })
            });
        loop {
            let mut attempted = None;
            let outcome = self
                .create_attempt(demand, waiting.clone(), candidates.clone(), &mut attempted)
                .await;
            let source = match outcome {
                Ok(client) => return Ok(client),
                Err(source) => source,
            };
            let retry = zero_platform_tokio::controlled_carrier::original_connect_error(&source)
                .is_some()
                && match &candidates {
                    Some(candidates) => candidates.has_next().await,
                    None => false,
                };
            if !retry {
                return Err(source);
            }
            let original =
                attempted.ok_or_else(|| error(self.instance, Stage::Close, Reason::Internal))?;
            // Same actual record, original native/users and real auxiliary Host
            // ACK must all settle before another reserve/native acquisition.
            self.wait(demand, self.reap_demand(demand.deadline)).await?;
            if !original.closed() {
                let mut failure =
                    error(self.instance, Stage::Close, Reason::Capacity).with_source(source);
                failure.delivery = DeliveryState::Unknown;
                return Err(failure);
            }
            // create_attempt's next original shares the same Managed/N and
            // TaskScope. No second DNS job, pool, driver or issuer is created.
        }
    }
}

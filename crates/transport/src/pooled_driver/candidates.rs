//! One actual demand resolves once; failed physical originals remain charged.
use std::sync::Arc;
use zero_core::request::Destination;
use zero_platform_tokio::{
    contract::{
        network::{CarrierContext, UpstreamTransport},
        ChannelError,
    },
    controlled_carrier::{CarrierCandidates, CarrierGrant, PreparedCarrier},
};
pub(super) struct Candidates {
    call: CarrierContext,
    state: tokio::sync::Mutex<Option<(CarrierCandidates, usize)>>,
}
impl Candidates {
    pub(super) fn new(call: CarrierContext) -> Arc<Self> {
        Arc::new(Self {
            call,
            state: tokio::sync::Mutex::new(None),
        })
    }
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
        *next += 1;
        Ok(prepared)
    }
    pub(super) async fn has_next(&self) -> bool {
        self.state
            .lock()
            .await
            .as_ref()
            .is_some_and(|(candidates, next)| *next < candidates.len())
    }
}

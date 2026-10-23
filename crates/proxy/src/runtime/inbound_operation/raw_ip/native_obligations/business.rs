//! The original raw TCP receiver accepts ordinary per-flow execution results.
use super::*;
use zero_engine::EngineError;

impl NativePermit {
    /// Preserve the existing raw TCP policy: a normal session execution error
    /// ends that flow, not the whole listener. Its original typed source stays
    /// in this bounded record with its exact real caller binding/witness.
    /// Setup failures and task panics still propagate through the actual Scope.
    /// This does not close, remove or release the native obligation.
    pub(crate) fn consume_business_result(
        &self,
        result: Result<(), EngineError>,
    ) -> Result<(), EngineError> {
        let Err(error) = result else {
            return Ok(());
        };
        let source = Arc::new(error);
        let mut state = self
            .owner
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let record = state.records.get_mut(&self.id).ok_or_else(|| {
            std::io::Error::other(rejected(Reason::Revoked).with_source(source.clone()))
        })?;
        if !record.entered
            || record
                .caller
                .as_ref()
                .and_then(|caller| caller.join_witness())
                .is_none()
            || record.business_failure.is_some()
        {
            return Err(
                std::io::Error::other(rejected(Reason::InvalidConfig).with_source(source)).into(),
            );
        }
        record.business_failure = Some(source.clone());
        drop(state);
        tracing::debug!(%source, "raw-IP TCP connection failed");
        Ok(())
    }
}

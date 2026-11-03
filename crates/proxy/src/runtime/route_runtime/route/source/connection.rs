//! Trusted prelude facts change admission addresses, never the native origin.
use super::super::model::InboundRouteRuntime;
use zero_engine::EngineError;
use zero_platform_tokio::contract::context::InboundConnectionFacts;
impl InboundRouteRuntime {
    pub(super) fn with_prepared_connection(
        mut self,
        facts: InboundConnectionFacts,
    ) -> Result<Self, EngineError> {
        if facts.original_source != self.connection_facts().original_source {
            return Err(std::io::Error::other(
                "prepared connection changed its original carrier identity",
            )
            .into());
        }
        self = self
            .with_source_addr(facts.effective_source)
            .with_local_addr(facts.local_endpoint);
        self.connection = Some(facts);
        Ok(self)
    }
}

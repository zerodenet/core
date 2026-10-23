//! One actual invocation, never retained as a pool's lifetime or control budget.
use zero_engine::EngineError;
use zero_platform_tokio::contract::{
    context::RequestIdentity,
    network::{CarrierContext, UpstreamBinding},
};

pub(crate) struct PacketCarrierDemand {
    pub(crate) identity: RequestIdentity,
    pub(crate) context: CarrierContext,
    pub(crate) binding: UpstreamBinding,
}
impl PacketCarrierDemand {
    pub(crate) fn validate(&self) -> Result<(), EngineError> {
        let valid = match &self.identity {
            RequestIdentity::Business(facts) => self.context.session == Some(facts.session),
            RequestIdentity::Service(_) => self.context.session.is_none(),
            RequestIdentity::Packet(_) => false,
        };
        if !valid {
            return Err(EngineError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "packet carrier demand identity differs from its actual caller",
            )));
        }
        if self.context.cancel.is_cancelled() {
            return Err(EngineError::Io(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "packet carrier demand cancelled",
            )));
        }
        if self
            .context
            .deadline
            .is_some_and(|end| end <= tokio::time::Instant::now())
        {
            return Err(EngineError::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "packet carrier demand expired",
            )));
        }
        Ok(())
    }
}

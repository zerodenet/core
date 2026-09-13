use super::InboundConnectionContext;

impl InboundConnectionContext {
    pub(crate) async fn serve_control_session(
        self,
        session: Box<dyn zero_core::inbound::InboundControlSession>,
    ) -> Result<(), zero_engine::EngineError> {
        super::super::control::run(self.runtime, session).await
    }
}

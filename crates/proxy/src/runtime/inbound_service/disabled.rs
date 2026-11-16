#[derive(Debug, Clone, Default)]
pub(crate) struct InboundServices;
impl InboundServices {
    pub(crate) async fn discard_prepared(&self) {}
    pub(crate) async fn prepare_restoration(
        &self,
        _: &crate::protocol_registry::TcpRuntimeServices,
    ) -> Result<(), zero_engine::EngineError> {
        Ok(())
    }
    pub(crate) async fn accept(&self) {}
    pub(crate) async fn suspend_for_replacement(&mut self) -> Result<(), zero_engine::EngineError> {
        Ok(())
    }
    pub(crate) async fn restore(
        &mut self,
        _: crate::protocol_registry::TcpRuntimeServices,
        _: tokio::sync::watch::Receiver<bool>,
    ) -> Result<(), zero_engine::EngineError> {
        Ok(())
    }
    pub(crate) async fn begin_run(&self) -> Result<(), zero_engine::EngineError> {
        Ok(())
    }

    pub(crate) fn recovery_permitted(&self) -> bool {
        true
    }

    pub(crate) async fn drive<F: std::future::Future>(&self, operation: F) -> F::Output {
        operation.await
    }

    pub(crate) async fn shutdown(&mut self) -> Result<(), zero_engine::EngineError> {
        Ok(())
    }

    pub(crate) fn is_empty(&self) -> bool {
        true
    }
    pub(crate) async fn join_next(
        &mut self,
    ) -> Option<Result<Result<(), zero_engine::EngineError>, tokio::task::JoinError>> {
        std::future::pending().await
    }
}

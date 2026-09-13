#[derive(Default)]
pub(crate) struct InboundServices;
impl InboundServices {
    pub(crate) fn is_empty(&self) -> bool {
        true
    }
    pub(crate) async fn join_next(
        &mut self,
    ) -> Option<Result<Result<(), zero_engine::EngineError>, tokio::task::JoinError>> {
        std::future::pending().await
    }
}

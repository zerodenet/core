use super::*;
use zero_transport::runner::GuardedCall;

impl Managed {
    pub(super) async fn wait<T>(
        &self,
        context: &OpenContext,
        stage: Stage,
        future: impl std::future::Future<Output = Result<T, ChannelError>>,
    ) -> Result<T, ChannelError> {
        let session = context.session_identity()?;
        let scope_cancel = self.tasks.cancellation();
        let call = GuardedCall::from_admitted(&self.admission, context, session);
        call.run_scoped(&scope_cancel, stage, DeliveryState::Unknown, future)
            .await?
    }
}

use super::*;

impl Managed {
    pub(super) async fn wait<T>(
        &self,
        context: &OpenContext,
        stage: Stage,
        future: impl std::future::Future<Output = Result<T, ChannelError>>,
    ) -> Result<T, ChannelError> {
        let session = context.session_identity()?;
        let scope_cancel = self.tasks.cancellation();
        tokio::select! { biased;
            _=self.admission.cancelled()=>Err(self.admission.error(session,stage,Reason::Revoked,DeliveryState::Unknown)),
            _=scope_cancel.cancelled()=>Err(self.admission.error(session,stage,Reason::Revoked,DeliveryState::Unknown)),
            _=context.cancel.cancelled()=>Err(self.admission.error(session,stage,Reason::Cancelled,DeliveryState::Unknown)),
            result=with_deadline(context.deadline,future)=>result.map_err(|source|
                self.admission.error(session,stage,Reason::Timeout,DeliveryState::Unknown).with_source(source))?,
        }
    }
}

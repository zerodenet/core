//! Reserve the receiver before any original or borrowed acquisition can await.
use super::*;
impl Outbound {
    pub(in crate::component) async fn acquire_original(
        &self,
        context: &OpenContext,
        session: Option<SessionId>,
        transferred: bool,
    ) -> Result<(CarrierIo, CarrierUserView, Attempt), ChannelError> {
        let carriers = self
            .carriers
            .as_ref()
            .expect("selected actual original association owner");
        let call = CarrierContext {
            session,
            deadline: context.deadline,
            cancel: context.cancel.clone(),
        };
        if let Some(end) = context.deadline {
            carriers.reap_returned(end).await?;
        }
        let mut grant = carriers.reserve()?;
        let view = if transferred {
            grant.claim_transferred_view()?
        } else {
            grant.claim_user_view()?
        };
        let original = grant.adoption();
        let attempt = Attempt(original.clone(), true);
        // The same CarrierOwner already retains the original and actual view
        // before callbacks, native acquisition or protocol authentication await.
        if let Some(waiting) = context.upstream.observed_pending_carrier() {
            waiting.retain(original.clone())?;
        }
        let acquiring = async {
            if session.is_none() && !context.upstream.binding().prefix.is_empty() {
                let loan = context
                    .upstream
                    .borrow_stream(self.options.endpoint.clone(), call.clone())
                    .await?;
                // The same installed receiver keeps the donor's unique logical
                // controller before authentication or CONNECT can suspend.
                grant.adopt_stream_loan(
                    context.upstream.binding(),
                    self.options.endpoint.clone(),
                    &call,
                    loan,
                )
            } else {
                context
                    .upstream
                    .acquire_controlled_stream(self.options.endpoint.clone(), call, grant)
                    .await
            }
        };
        let io=tokio::select! {biased;
            _=self.admission.cancelled()=>return Err(self.admission.error(session,Stage::Dial,Reason::Revoked,DeliveryState::Unknown)),
            _=context.cancel.cancelled()=>return Err(self.admission.error(session,Stage::Dial,Reason::Cancelled,DeliveryState::Unknown)),
            result=with_deadline(context.deadline,acquiring)=>result.map_err(|source|self.admission.error(session,Stage::Dial,Reason::Timeout,DeliveryState::Unknown).with_source(source))?,
        }.map_err(|error|self.admission.attach(error,session))?;
        Ok((io, view, attempt))
    }
}

//! MUX business streams and same-owner target-free reverse control.
use super::*;
impl StreamOutbound for Outbound {
    type Stream = BoxStream;
    async fn connect(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> Result<Connected<BoxStream>, ChannelError> {
        if crate::component::request::dialed_ingress(&context) {
            return self
                .control_stream(&context)?
                .connect(request, context)
                .await;
        }
        let io = self.connect_actual(request, context).await?;
        Ok(Connected {
            io: io.io,
            peer_confirmation: PeerConfirmation::Deferred,
        })
    }
    async fn connect_carrier(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> Result<Connected<zero_platform_tokio::controlled_carrier::loan::StreamLoan>, ChannelError>
    {
        if crate::component::request::dialed_ingress(&context) {
            return self
                .control_stream(&context)?
                .connect_carrier(request, context)
                .await;
        }
        let session = context.session_identity()?;
        let io = self.connect_actual(request, context).await?;
        let loan = io
            .into_stream_loan()
            .map_err(|error| self.admission.attach(error, session))?;
        Ok(Connected {
            io: loan,
            peer_confirmation: PeerConfirmation::Deferred,
        })
    }
}
impl Outbound {
    fn control_stream(
        &self,
        context: &OpenContext,
    ) -> Result<Arc<dyn zero_platform_tokio::contract::stream::DynStreamOutbound>, ChannelError>
    {
        let session = self.admission.check(context)?;
        self.control_stream
            .as_ref()
            .and_then(Weak::upgrade)
            .ok_or_else(|| {
                self.admission.error(
                    session,
                    Stage::Prepare,
                    Reason::Unsupported,
                    DeliveryState::NotSubmitted,
                )
            })
    }

    async fn connect_actual(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> Result<zero_transport::pooled_driver::PooledStream, ChannelError> {
        let session = self.admission.check(&context)?;
        let leaf = self.selected_leaf(&context)?;
        let opening = leaf.open_native_mux(request.destination, &context);
        let io = tokio::select! { biased;
            _=self.admission.cancelled()=>return Err(self.admission.error(session,Stage::Handshake,Reason::Revoked,DeliveryState::Unknown)),
            _=context.cancel.cancelled()=>return Err(self.admission.error(session,Stage::Handshake,Reason::Cancelled,DeliveryState::Unknown)),
            result=with_deadline(context.deadline,opening)=>result.map_err(|source|self.admission.error(session,Stage::Handshake,Reason::Timeout,DeliveryState::Unknown).with_source(source))?,
        }.map_err(|source|self.admission.error(session,Stage::Handshake,Reason::Io,DeliveryState::Unknown).with_source(source))?;
        if !context.upstream.observe_carrier_origin(&io.origin) {
            return Err(self.admission.error(
                session,
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::Unknown,
            ));
        }
        if let Some(observer) = &context.observer {
            observer.record(session, ConnectFact::CarrierReady);
        }
        Ok(io)
    }
    fn selected_leaf(
        &self,
        context: &OpenContext,
    ) -> Result<crate::transport::VlessOutboundLeaf, ChannelError> {
        let session = self.admission.check(context)?;
        let options = self.options.upgrade().ok_or_else(|| {
            self.admission.error(
                session,
                Stage::Prepare,
                Reason::Revoked,
                DeliveryState::NotSubmitted,
            )
        })?;
        let ready = self.ready.upgrade().ok_or_else(|| {
            self.admission.error(
                session,
                Stage::Prepare,
                Reason::Revoked,
                DeliveryState::NotSubmitted,
            )
        })?;
        let leaf = ready.leaf.lock().unwrap().clone().ok_or_else(|| {
            self.admission.error(
                session,
                Stage::Prepare,
                Reason::Revoked,
                DeliveryState::NotSubmitted,
            )
        })?;
        if leaf.native_mux_instance().ok() != Some(self.pool)
            || context.upstream.binding().dial != options.prepared.pool.binding().dial
        {
            return Err(self.admission.error(
                session,
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            ));
        }
        Ok(leaf)
    }
}

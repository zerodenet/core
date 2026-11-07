use super::*;
use zero_platform_tokio::contract::stream::{BoxStream, ConnectRequest, Connected, StreamOutbound};
impl StreamOutbound for Outbound {
    type Stream = BoxStream;
    async fn connect(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> Result<Connected<BoxStream>, ChannelError> {
        let owner = &self.0;
        let session = owner.admission.check(&context)?;
        let (io, _) = owner.open_transport(&request.destination, &context).await?;
        crate::component::submit::submit(
            &owner.options.base,
            &owner.admission,
            request,
            context,
            session,
            io,
        )
        .await
    }
    async fn connect_carrier(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> Result<Connected<zero_platform_tokio::controlled_carrier::loan::StreamLoan>, ChannelError>
    {
        let owner = &self.0;
        let session = owner.admission.check(&context)?;
        if session.is_some() || !owner.options.node_streams {
            return Err(owner.admission.error(
                session,
                Stage::Prepare,
                Reason::InvalidConfig,
                DeliveryState::NotSubmitted,
            ));
        }
        let (io, record) = owner.open_transport(&request.destination, &context).await?;
        let witness = record
            .witness
            .lock()
            .unwrap()
            .clone()
            .expect("actual H2 driver registered before view");
        let original = record.adoption.origin(&witness)?;
        let connected = crate::component::submit::submit(
            &owner.options.base,
            &owner.admission,
            request,
            context,
            session,
            io,
        )
        .await?;
        Ok(Connected {
            io: zero_platform_tokio::controlled_carrier::loan::StreamLoan::new(
                connected.io,
                Box::new(Control(record)).into(),
                original,
            ),
            peer_confirmation: connected.peer_confirmation,
        })
    }
}
struct Control(Arc<Record>);
impl zero_platform_tokio::contract::lifecycle::DynChannelControl for Control {
    fn cancel(&self, _: zero_platform_tokio::contract::CloseReason) {
        self.0.stop.cancel();
        self.0.adoption.request_retirement();
    }
    fn wait_closed(
        &mut self,
        end: Instant,
    ) -> zero_platform_tokio::contract::BoxFuture<
        '_,
        Result<zero_platform_tokio::contract::lifecycle::CloseReport, ChannelError>,
    > {
        Box::pin(async move {
            self.0.stop.cancel();
            self.0.adoption.request_retirement();
            self.0.retire(end).await
        })
    }
}

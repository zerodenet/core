use super::*;
use zero_core::request::DatagramTargets;
use zero_platform_tokio::contract::{
    datagram::{BoxDatagramSession, DatagramRequest, DynDatagramOutbound},
    BoxFuture,
};
use zero_platform_tokio::controlled_carrier::loan::DatagramLoan;
impl Carrier {
    async fn datagram(
        &self,
        request: DatagramRequest,
        context: OpenContext,
    ) -> Result<
        (
            BoxDatagramSession,
            CarrierOrigin,
            crate::runtime::packet_flow::FlowReceiverBinding,
        ),
        ChannelError,
    > {
        context.session_identity()?;
        let acquiring = self.acquisition()?;
        let DatagramTargets::Fixed(target) = &request.targets else {
            return Err(error(Stage::Prepare, Reason::InvalidConfig));
        };
        let peer = self.resolve(target, &context).await?;
        let original = self.origin_for(peer.ip())?;
        let (device, local) = self.device(peer.ip())?;
        let borrowed = self.flow_borrow(&context)?;
        let (session, receiver) = device.open_standard_datagram(
            &self.scope,
            local,
            peer,
            request,
            context.clone(),
            borrowed,
        )?;
        drop(acquiring);
        self.observe_peer(peer.ip(), &context)?;
        Ok((session, original, receiver))
    }
}
impl DynDatagramOutbound for Carrier {
    fn open(
        &self,
        request: DatagramRequest,
        context: OpenContext,
    ) -> BoxFuture<'_, Result<BoxDatagramSession, ChannelError>> {
        Box::pin(async move {
            self.datagram(request, context)
                .await
                .map(|(session, _, _)| session)
        })
    }
    fn open_carrier(
        &self,
        request: DatagramRequest,
        context: OpenContext,
    ) -> BoxFuture<'_, Result<DatagramLoan, ChannelError>> {
        Box::pin(async move {
            let association = request.association;
            let DatagramTargets::Fixed(target) = &request.targets else {
                return Err(error(Stage::Prepare, Reason::InvalidConfig));
            };
            let target = target.clone();
            let (session, original, receiver) = self.datagram(request, context).await?;
            let mut loan = DatagramLoan::new(session, original, association, target);
            let observation = loan.receiver_observation();
            receiver.bind(observation, &mut loan.owner)?;
            Ok(loan)
        })
    }
}

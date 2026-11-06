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
    ) -> Result<BoxDatagramSession, ChannelError> {
        context.session_identity()?;
        let DatagramTargets::Fixed(target) = &request.targets else {
            return Err(error(Stage::Prepare, Reason::InvalidConfig));
        };
        let peer = self.resolve(target, &context).await?;
        let (device, local) = self.device(peer.ip())?;
        self.observe_peer(peer.ip(), &context)?;
        device.open_standard_datagram(&self.scope, local, peer, request, context.cancel)
    }
}
impl DynDatagramOutbound for Carrier {
    fn open(
        &self,
        request: DatagramRequest,
        context: OpenContext,
    ) -> BoxFuture<'_, Result<BoxDatagramSession, ChannelError>> {
        Box::pin(async move {
            self.origin()?;
            self.datagram(request, context).await
        })
    }
    fn open_carrier(
        &self,
        request: DatagramRequest,
        context: OpenContext,
    ) -> BoxFuture<'_, Result<DatagramLoan, ChannelError>> {
        Box::pin(async move {
            let original = self.origin()?;
            let association = request.association;
            let DatagramTargets::Fixed(target) = &request.targets else {
                return Err(error(Stage::Prepare, Reason::InvalidConfig));
            };
            let target = target.clone();
            let session = self.datagram(request, context).await?;
            Ok(DatagramLoan::new(session, original, association, target))
        })
    }
}

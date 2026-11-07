use super::*;
use zero_platform_tokio::contract::{
    stream::{BoxStream, ConnectRequest, Connected, DynStreamOutbound},
    BoxFuture,
};
use zero_platform_tokio::controlled_carrier::loan::StreamLoan;
impl DynStreamOutbound for Carrier {
    fn connect(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> BoxFuture<'_, Result<Connected<BoxStream>, ChannelError>> {
        Box::pin(async move {
            context.session_identity()?;
            let acquiring = self.acquisition()?;
            let peer = self.resolve(&request.destination, &context).await?;
            let original = self.origin_for(peer.ip())?;
            let (device, local) = self.device(peer.ip())?;
            let borrowed = self.flow_borrow(&context)?;
            let caller = borrowed.caller.clone();
            let connected =
                device.open_standard_stream(&self.scope, local, peer, context.clone(), borrowed)?;
            // Both the real user record and this unique cold opening/control
            // now exist. Pausing sees that custody; existing IO is not revoked.
            drop(acquiring);
            self.observe_peer(peer.ip(), &context)?;
            let io = connected.io.establish().await?;
            let current = self.origin_for(peer.ip()).and_then(|current| {
                if !current.same_original(&original)
                    || !self
                        .flow_borrow(&context)?
                        .caller
                        .same_registration(&caller)
                {
                    return Err(error(Stage::Dial, Reason::StalePlan));
                }
                Ok(())
            });
            if let Err(source) = current {
                io.close_failed().await;
                return Err(source);
            }
            Ok(Connected {
                io: Box::pin(io) as BoxStream,
                peer_confirmation:
                    zero_platform_tokio::contract::stream::PeerConfirmation::Confirmed,
            })
        })
    }
    fn connect_carrier(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> BoxFuture<'_, Result<Connected<StreamLoan>, ChannelError>> {
        Box::pin(async move {
            context.session_identity()?;
            let acquiring = self.acquisition()?;
            let peer = self.resolve(&request.destination, &context).await?;
            let original = self.origin_for(peer.ip())?;
            let (device, local) = self.device(peer.ip())?;
            let borrowed = self.flow_borrow(&context)?;
            let connected =
                device.open_standard_stream(&self.scope, local, peer, context.clone(), borrowed)?;
            // Both the real user record and this unique cold opening/control
            // now exist. Pausing sees that custody; existing IO is not revoked.
            drop(acquiring);
            self.observe_peer(peer.ip(), &context)?;
            Ok(Connected {
                io: connected.io.into_loan(original)?,
                peer_confirmation: connected.peer_confirmation,
            })
        })
    }
}

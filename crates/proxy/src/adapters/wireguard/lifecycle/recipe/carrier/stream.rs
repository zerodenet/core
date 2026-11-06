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
            self.origin()?;
            let peer = self.resolve(&request.destination, &context).await?;
            let (device, local) = self.device(peer.ip())?;
            self.observe_peer(peer.ip(), &context)?;
            let connected = device.open_standard_stream(&self.scope, local, peer, context)?;
            Ok(Connected {
                io: Box::pin(connected.io.establish().await?) as BoxStream,
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
            let original = self.origin()?;
            let peer = self.resolve(&request.destination, &context).await?;
            let (device, local) = self.device(peer.ip())?;
            self.observe_peer(peer.ip(), &context)?;
            let connected = device.open_standard_stream(&self.scope, local, peer, context)?;
            Ok(Connected {
                io: connected.io.into_loan(original),
                peer_confirmation: connected.peer_confirmation,
            })
        })
    }
}

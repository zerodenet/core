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
}

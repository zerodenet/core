use zero_core::Session;
use zero_engine::EngineError;

use super::model::InboundRouteRuntime;
use crate::runtime::tcp_ingress::InboundProtocol;

impl InboundRouteRuntime {
    pub(crate) async fn serve<P>(
        &self,
        session: Session,
        client: P::ClientStream,
        protocol: &P,
    ) -> Result<(), EngineError>
    where
        P: InboundProtocol + 'static,
    {
        self.tcp_runtime.serve(session, client, protocol).await
    }
}

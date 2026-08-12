use zero_core::Session;
use zero_engine::EngineError;

use super::model::InboundRouteRuntime;
use crate::runtime::tcp_ingress::InboundProtocol;

impl InboundRouteRuntime {
    pub(crate) async fn serve_message<P>(
        &self,
        protocol: &P,
        client: &mut crate::transport::TcpRelayStream,
        request: P::Request,
    ) -> Result<crate::runtime::tcp_ingress::MessageRelayOutcome, EngineError>
    where
        P: crate::runtime::tcp_ingress::MessageInboundProtocol,
    {
        self.tcp_runtime
            .serve_message(protocol, client, request)
            .await
    }

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

    #[cfg(any(
        feature = "upstream-association-runtime",
        feature = "managed-datagram-runtime",
        feature = "managed-stream-runtime"
    ))]
    pub(crate) async fn serve_with_client_response<P, S>(
        &self,
        session: Session,
        client: S,
        response_protocol: P,
    ) -> Result<(), EngineError>
    where
        P: zero_core::InboundClientResponse<S> + Send + Sync,
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + zero_traits::AsyncSocket + Unpin + Send,
    {
        self.tcp_runtime
            .serve_with_client_response(session, client, response_protocol)
            .await
    }

    #[cfg(feature = "managed-stream-runtime")]
    pub(crate) async fn relay_recorded_fallback_replay<R>(
        &self,
        fallback: crate::runtime::InboundFallbackTarget,
        replay: R,
    ) -> Result<(), EngineError>
    where
        R: zero_core::InboundFallbackReplay + 'static,
        R::Stream: crate::transport::ClientStream,
    {
        self.tcp_runtime
            .relay_recorded_fallback_replay(fallback, replay)
            .await
    }
}

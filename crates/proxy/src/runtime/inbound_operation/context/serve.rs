use zero_engine::EngineError;

use super::model::InboundConnectionContext;

impl InboundConnectionContext {
    pub(crate) async fn serve_message<P>(
        &self,
        protocol: &P,
        client: &mut crate::transport::TcpRelayStream,
        request: P::Request,
    ) -> Result<crate::runtime::tcp_ingress::MessageRelayOutcome, EngineError>
    where
        P: crate::runtime::tcp_ingress::MessageInboundProtocol,
    {
        self.runtime.serve_message(protocol, client, request).await
    }

    pub(crate) async fn serve<P>(
        self,
        session: zero_core::Session,
        client: P::ClientStream,
        protocol: P,
    ) -> Result<(), EngineError>
    where
        P: crate::runtime::tcp_ingress::InboundProtocol + 'static,
    {
        self.runtime.serve(session, client, &protocol).await
    }

    pub(crate) fn select_http_redirect(
        &self,
        session: &zero_core::Session,
    ) -> Option<(u16, String)> {
        self.runtime.select_http_redirect(session)
    }

    #[cfg(any(
        feature = "upstream-association-runtime",
        feature = "managed-datagram-runtime",
        feature = "managed-stream-runtime"
    ))]
    pub(crate) async fn serve_with_client_response<P, S>(
        self,
        session: zero_core::Session,
        client: S,
        response_protocol: P,
    ) -> Result<(), EngineError>
    where
        P: zero_core::InboundClientResponse<S> + Send + Sync,
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + zero_traits::AsyncSocket + Unpin + Send,
    {
        self.runtime
            .serve_with_client_response(session, client, response_protocol)
            .await
    }
}

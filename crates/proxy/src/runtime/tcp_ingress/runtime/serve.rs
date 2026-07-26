use zero_core::Session;
use zero_engine::EngineError;

#[cfg(feature = "managed-stream-runtime")]
use crate::runtime::pipe::{KernelPipe, TcpPipe, TcpPipeInput};
#[cfg(feature = "managed-stream-runtime")]
use crate::transport::TcpRouteResult;

use super::super::contract::InboundProtocol;
use super::super::lifecycle::serve_inbound;
use super::model::TcpIngressRuntime;

impl TcpIngressRuntime {
    pub(crate) async fn serve<P>(
        &self,
        session: Session,
        client: P::ClientStream,
        protocol: &P,
    ) -> Result<(), EngineError>
    where
        P: InboundProtocol + 'static,
    {
        serve_inbound(self, session, client, protocol).await
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
        super::super::lifecycle::serve_inbound_with_client_response(
            self,
            session,
            client,
            response_protocol,
        )
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
        crate::runtime::inbound_fallback::relay_recorded_fallback_replay(
            self.runtime_services(),
            fallback,
            replay,
        )
        .await
    }

    #[cfg(feature = "managed-stream-runtime")]
    pub(crate) async fn open_tcp_upstream(
        &self,
        session: &mut Session,
    ) -> Result<TcpRouteResult, EngineError> {
        self.prepare_session(session)?;
        TcpPipe::new(self).dispatch(TcpPipeInput { session }).await
    }
}

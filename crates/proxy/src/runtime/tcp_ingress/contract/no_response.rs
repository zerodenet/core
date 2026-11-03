use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite};
use zero_engine::EngineError;

use super::protocol::InboundProtocol;
#[cfg(all(test, feature = "managed-stream-runtime"))]
use crate::transport::TcpRelayStream;

#[derive(Clone, Copy, Default)]
pub(crate) struct NoClientResponseStreamProtocol<S> {
    _stream: core::marker::PhantomData<fn() -> S>,
}

impl<S> NoClientResponseStreamProtocol<S> {
    pub(crate) const fn new() -> Self {
        Self {
            _stream: core::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<S> InboundProtocol for NoClientResponseStreamProtocol<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    type ClientStream = S;

    async fn send_ok(&self, _client: &mut Self::ClientStream) -> Result<(), EngineError> {
        Ok(())
    }

    async fn send_blocked(&self, _client: &mut Self::ClientStream) -> Result<(), EngineError> {
        Ok(())
    }

    async fn send_upstream_failure(
        &self,
        _client: &mut Self::ClientStream,
    ) -> Result<(), EngineError> {
        Ok(())
    }
}

#[derive(Clone, Copy, Default)]
#[cfg(all(test, feature = "managed-stream-runtime"))]
pub(crate) struct NoClientResponseInboundProtocol;

#[async_trait]
#[cfg(all(test, feature = "managed-stream-runtime"))]
impl InboundProtocol for NoClientResponseInboundProtocol {
    type ClientStream = TcpRelayStream;

    async fn send_ok(&self, _client: &mut TcpRelayStream) -> Result<(), EngineError> {
        Ok(())
    }

    async fn send_blocked(&self, _client: &mut TcpRelayStream) -> Result<(), EngineError> {
        Ok(())
    }

    async fn send_upstream_failure(&self, _client: &mut TcpRelayStream) -> Result<(), EngineError> {
        Ok(())
    }
}

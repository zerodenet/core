use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite};
use zero_engine::EngineError;

use super::protocol::InboundProtocol;

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

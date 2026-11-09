use super::*;
use zero_platform_tokio::TokioDatagramSocket;
pub(crate) struct DirectRawIpWireCarrier {
    original: tokio::sync::RwLock<Option<TokioDatagramSocket>>,
    close: std::sync::Mutex<Option<Result<(), Arc<io::Error>>>>,
}
impl DirectRawIpWireCarrier {
    pub(crate) fn new(socket: TokioDatagramSocket) -> Self {
        Self {
            original: tokio::sync::RwLock::new(Some(socket)),
            close: std::sync::Mutex::new(None),
        }
    }
}
#[async_trait]
impl RawIpWireCarrier for DirectRawIpWireCarrier {
    fn background_drained(&self) -> bool {
        true
    }
    fn stop_background(&self) -> Option<CarrierWorkerCompletion> {
        Some(Box::pin(async { Ok(()) }))
    }
    async fn close_native(&self) -> Result<(), EngineError> {
        // Worker joins precede this exclusive take. No retained Arc view owns
        // a second socket; cancellation cannot discard the original result.
        let mut original = self.original.write().await;
        let mut result = self.close.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(socket) = original.take() {
            *result = Some(socket.close().map_err(Arc::new));
        }
        match result.as_ref() {
            Some(Ok(())) => Ok(()),
            Some(Err(error)) => Err(EngineError::Io(io::Error::new(
                error.kind(),
                NativeCloseFailure(error.clone()),
            ))),
            None => Err(EngineError::Io(io::Error::other(
                "raw-IP original close outcome missing",
            ))),
        }
    }
    async fn recv(&self, buf: &mut [u8]) -> io::Result<(usize, Option<SocketAddr>)> {
        let guard = self.original.read().await;
        let socket = guard.as_ref().ok_or_else(background_stopped)?;
        socket
            .recv_from_addr(buf)
            .await
            .map(|(n, source)| (n, Some(source)))
    }
    async fn send(&self, packet: &[u8], endpoint: SocketAddr) -> io::Result<()> {
        let guard = self.original.read().await;
        let socket = guard.as_ref().ok_or_else(background_stopped)?;
        socket.send_to_addr(packet, endpoint).await.map(|_| ())
    }
}

#[derive(Debug)]
struct NativeCloseFailure(Arc<io::Error>);
impl std::fmt::Display for NativeCloseFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for NativeCloseFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

#[cfg(test)]
mod tests;

//! Actual owning receiver, original Scope identity and exit dispatch.
use super::*;
impl InboundOwner {
    pub(in crate::runtime::inbound_owners) fn accepted_registrar(&self) -> &TaskRegistrar {
        &self.registrar
    }
    pub(in crate::runtime::inbound_owners) fn install_accepted_receiver(
        &self,
        receiver: zero_platform_tokio::AcceptedTcpTaskReceiver,
    ) -> Result<(), EngineError> {
        self.accepted_receiver
            .set(receiver)
            .map_err(|_| std::io::Error::other("accepted TCP receiver already installed").into())
    }
    pub(crate) async fn observe_driver(&self) -> Result<(), EngineError> {
        if self.driver.get().is_none() {
            return Err(std::io::Error::other("inbound driver was not accepted").into());
        }
        let mut scope = self.scope.lock().await;
        #[cfg(feature = "managed-datagram-runtime")]
        {
            self.observe_peer_driver(&mut scope).await
        }
        #[cfg(not(feature = "managed-datagram-runtime"))]
        {
            while !self.driver_finished.load(Ordering::Acquire) {
                let exit = scope.next_exit().await.ok_or_else(|| {
                    std::io::Error::other("listener driver exit remains unobserved")
                })?;
                let failed = self.record(exit); // persist the actual ID/source before any next await
                if failed {
                    self.stop();
                }
            }
            self.result()
        }
    }
}

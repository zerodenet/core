//! Unique original bound view, with native closure retained by HostResources.
use super::*;
use zero_platform_tokio::{
    contract::component::BoundDatagramSocket, controlled_datagram::provider::NativeDatagramOrigin,
};
pub(crate) struct HostedRawIpWireCarrier {
    view: tokio::sync::RwLock<Option<BoundDatagramSocket>>,
    origin: std::sync::OnceLock<NativeDatagramOrigin>,
}
impl HostedRawIpWireCarrier {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            view: tokio::sync::RwLock::new(None),
            origin: std::sync::OnceLock::new(),
        })
    }
    pub(crate) async fn install(
        &self,
        view: BoundDatagramSocket,
        driver: &zero_platform_tokio::contract::scope::TaskJoinWitness,
    ) -> io::Result<()> {
        let actual = view
            .original()
            .ok_or_else(|| io::Error::other("bound original has no retained provider"))?;
        if !actual.driver().same_registration(driver) {
            return Err(io::Error::other(
                "bound original names another actual wire driver",
            ));
        }
        let mut original = self.view.write().await;
        if original.is_some() || self.origin.get().is_some() {
            return Err(io::Error::other(
                "same bound wire view was already handed off",
            ));
        }
        self.origin
            .set(actual)
            .map_err(|_| io::Error::other("same bound wire origin was already handed off"))?;
        *original = Some(view);
        Ok(())
    }
    pub(crate) fn original(&self) -> Option<NativeDatagramOrigin> {
        self.origin.get().cloned()
    }
    /// The caller first consumes that same driver's actual Scope witness.
    /// This returns the unique view, independently of physical native closure.
    pub(crate) async fn return_view(&self) {
        let returned = self.view.write().await.take();
        drop(returned);
    }
}
#[async_trait]
impl RawIpWireCarrier for HostedRawIpWireCarrier {
    fn background_drained(&self) -> bool {
        true
    }
    fn stop_background(&self) -> Option<CarrierWorkerCompletion> {
        Some(Box::pin(async { Ok(()) }))
    }
    async fn close_native(&self) -> Result<(), EngineError> {
        let actual = self
            .origin
            .get()
            .ok_or_else(|| io::Error::other("same hosted wire never issued a native original"))?;
        if actual
            .closed()
            .is_some_and(|closed| closed.matches(actual.owner()))
        {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "same HostResources native original is not confirmed closed",
            )
            .into())
        }
    }
    async fn recv(&self, buffer: &mut [u8]) -> io::Result<(usize, Option<SocketAddr>)> {
        let original = self.view.read().await;
        let view = original.as_ref().ok_or_else(background_stopped)?;
        view.recv_from_addr(buffer)
            .await
            .map(|(length, source)| (length, Some(source)))
    }
    async fn send(&self, packet: &[u8], endpoint: SocketAddr) -> io::Result<()> {
        let original = self.view.read().await;
        let view = original.as_ref().ok_or_else(background_stopped)?;
        let sent = view.send_to_addr(packet, endpoint).await?;
        if sent != packet.len() {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "same bound wire wrote a partial datagram",
            ));
        }
        Ok(())
    }
}

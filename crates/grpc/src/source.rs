//! Concrete production readers over the same kernel; no alternative executor.
#[derive(Default)]
pub(super) struct OriginalSource {
    pub(super) engine: Option<zero_engine::EngineHandle>,
    #[cfg(feature = "proxy-control")]
    pub(super) proxy: Option<Box<dyn std::any::Any + Send + Sync>>,
}
impl OriginalSource {
    #[cfg(feature = "proxy-control")]
    pub(super) fn proxy(&self) -> Option<&zero_proxy::ProxyHandle> {
        // This private field is constructed only from the concrete production
        // ProxyHandle. Erase its storage at this boundary so tonic's generic
        // Send/Sync proof does not recursively expand the entire host graph.
        // This is no provider/query SPI and offers no alternative executor.
        self.proxy.as_ref().map(|actual| {
            actual
                .downcast_ref()
                .expect("same concrete production ProxyHandle")
        })
    }
}

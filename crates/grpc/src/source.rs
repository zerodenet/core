//! Concrete production readers over the same kernel; no alternative executor.
#[derive(Default)]
pub(super) struct OriginalSource {
    pub(super) engine: Option<zero_engine::EngineHandle>,
    #[cfg(feature = "proxy-control")]
    pub(super) proxy: Option<zero_proxy::ProxyHandle>,
}

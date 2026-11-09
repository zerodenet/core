//! Same process logger book, supplied by main; no application fallback owner.
use super::*;
impl ApplicationServices {
    #[cfg(all(test, feature = "sink-jsonl"))]
    pub(super) async fn start_with_proxy(
        proxy: &zero_proxy::Proxy,
        ipc_hook_socket: Option<&str>,
    ) -> Result<Arc<Self>, Box<dyn Error>> {
        Self::start_with_proxy_and_logging(proxy, ipc_hook_socket, None).await
    }

    pub(super) fn prepare_logging(
        &self,
        config: &zero_config::LogConfig,
    ) -> std::io::Result<Option<zero_logging::LoggingBudgetCandidate>> {
        let Some(logging) = &self.logging else {
            return Ok(None);
        };
        if logging.limits_match(config)? {
            return Ok(None);
        }
        logging.prepare_limits(config).map(Some)
    }
}

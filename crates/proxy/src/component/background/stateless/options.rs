use super::*;
pub(crate) struct Options<T> {
    pub(super) inner: Arc<T>,
    pub(super) cleanup_network: Option<Arc<crate::component::background::BackgroundNetwork>>,
    pub(super) endpoint: Destination,
    pub(super) dial: zero_traits::DialPolicy,
    pub(super) limit: NonZeroUsize,
    pub(super) node_dns: Option<zero_core::component::ComponentId>,
    pub(super) process: Option<(zero_core::component::ComponentId, std::time::Duration)>,
    pub(super) preparation_jobs: bool,
}
impl<T> Options<T> {
    /// Borrow the same protocol-owned immutable recipe, never a new executor.
    pub(crate) fn inner_options(&self) -> &T {
        &self.inner
    }

    pub(crate) fn new(
        inner: T,
        endpoint: Destination,
        dial: zero_traits::DialPolicy,
        limit: NonZeroUsize,
    ) -> Self {
        Self {
            inner: Arc::new(inner),
            cleanup_network: None,
            endpoint,
            dial,
            limit,
            node_dns: None,
            process: None,
            preparation_jobs: false,
        }
    }
    /// Installed only by the actual protocol's owning preparation producer.
    pub(crate) fn with_preparation_jobs(mut self) -> Self {
        self.preparation_jobs = true;
        self
    }
    pub(crate) fn with_cleanup_network(mut self, network: Arc<crate::component::background::BackgroundNetwork>) -> Self {
        self.cleanup_network = Some(network);
        self
    }
    pub(crate) fn with_process(
        mut self,
        dependency: zero_core::component::ComponentId,
        startup: std::time::Duration,
    ) -> Self {
        self.process = Some((dependency, startup));
        self
    }
    pub(crate) fn with_node_dns(mut self, dependency: zero_core::component::ComponentId) -> Self {
        self.node_dns = Some(dependency);
        self
    }
}
impl<T> Clone for Options<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            cleanup_network: self.cleanup_network.clone(),
            endpoint: self.endpoint.clone(),
            dial: self.dial.clone(),
            limit: self.limit,
            node_dns: self.node_dns,
            process: self.process,
            preparation_jobs: self.preparation_jobs,
        }
    }
}

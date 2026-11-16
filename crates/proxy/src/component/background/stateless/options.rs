use super::*;
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum WrappedRoles {
    Both,
    Stream,
    Datagram,
    Control,
}
pub(crate) struct Options<T> {
    pub(super) inner: Arc<T>,
    pub(super) cleanup_network: Option<Arc<crate::component::background::BackgroundNetwork>>,
    pub(super) endpoint: Destination,
    pub(super) dial: zero_traits::DialPolicy,
    pub(super) limit: NonZeroUsize,
    pub(super) node_dns: Option<zero_core::component::ComponentId>,
    pub(super) process: Option<(zero_core::component::ComponentId, std::time::Duration)>,
    pub(super) preparation_jobs: bool,
    pub(super) roles: WrappedRoles,
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
            roles: WrappedRoles::Both,
        }
    }
    /// Reuse the exact already-prepared custody options around a protocol's
    /// combined role recipe. This performs no decode or resource acquisition.
    pub(crate) fn map_inner<U>(self, map: impl FnOnce(Arc<T>) -> U) -> Options<U> {
        Options {
            inner: Arc::new(map(self.inner)),
            cleanup_network: self.cleanup_network,
            endpoint: self.endpoint,
            dial: self.dial,
            limit: self.limit,
            node_dns: self.node_dns,
            process: self.process,
            preparation_jobs: self.preparation_jobs,
            roles: self.roles,
        }
    }
    pub(crate) fn for_control_only(mut self) -> Self {
        self.roles = WrappedRoles::Control;
        self
    }
    pub(crate) fn for_datagram_only(mut self) -> Self {
        self.roles = WrappedRoles::Datagram;
        self
    }

    pub(crate) fn for_stream_only(mut self) -> Self {
        self.roles = WrappedRoles::Stream;
        self
    }

    /// Installed only by the actual protocol's owning preparation producer.
    pub(crate) fn with_preparation_jobs(mut self) -> Self {
        self.preparation_jobs = true;
        self
    }
    pub(crate) fn with_cleanup_network(
        mut self,
        network: Arc<crate::component::background::BackgroundNetwork>,
    ) -> Self {
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
            roles: self.roles,
        }
    }
}

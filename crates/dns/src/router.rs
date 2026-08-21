//! DNS server dispatch backed by the kernel's shared domain condition model.

use zero_router::DomainDispatcher;

#[derive(Debug, Clone)]
pub(crate) struct DnsDispatcher {
    inner: DomainDispatcher<String>,
}

impl DnsDispatcher {
    pub(crate) fn new(inner: DomainDispatcher<String>) -> Self {
        Self { inner }
    }

    /// Select exactly one named backend. Dispatch order is first-match-wins.
    pub(crate) fn select(&self, domain: &str) -> &str {
        self.inner.select(domain)
    }
}

//! Resolver facts for runtime services. Ordinary protocols receive their selected
//! UpstreamTransport instead of gaining an alternate path to the network.
use super::{BoxFuture, CancellationToken, ChannelError};
use tokio::time::Instant;
use zero_core::component::NetworkGeneration;
use zero_traits::{AddressFamily, IpAddress};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DnsQueryRole {
    Default,
    Direct,
    Node,
    /// Explicit endpoint bootstrap, isolated from ordinary server lookups.
    /// The host must configure a non-recursive node/bootstrap policy.
    Bootstrap,
}
impl DnsQueryRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::Direct => "direct",
            Self::Node => "node",
            Self::Bootstrap => "bootstrap",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DnsGeneration {
    pub resolver: u64,
    pub network: NetworkGeneration,
}
impl DnsGeneration {
    pub fn new(resolver: u64, network: u64) -> Self {
        Self {
            resolver,
            network: NetworkGeneration(network),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DnsRequest {
    pub name: String,
    pub role: DnsQueryRole,
    pub family: AddressFamily,
}
impl DnsRequest {
    pub fn new(name: impl Into<String>, role: DnsQueryRole) -> Self {
        Self {
            name: name.into(),
            role,
            family: AddressFamily::Auto,
        }
    }
}

#[derive(Clone, Default)]
pub struct DnsQueryContext {
    /// Additional caller bound; configured per-backend timers remain active.
    pub deadline: Option<Instant>,
    pub cancel: CancellationToken,
    pub expected_generation: Option<DnsGeneration>,
}

#[derive(Debug, Clone)]
pub struct DnsResolution {
    /// Real addresses only. Client Fake-IP allocation stays with DNS wire APIs.
    pub addresses: Vec<IpAddress>,
    pub generation: DnsGeneration,
}

pub trait DnsService: Send + Sync {
    fn generation(&self) -> DnsGeneration;
    fn resolve(
        &self,
        request: DnsRequest,
        context: DnsQueryContext,
    ) -> BoxFuture<'_, Result<DnsResolution, ChannelError>>;
}

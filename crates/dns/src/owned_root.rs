//! Explicit library wiring; grants refer weakly to one original HostResources.
use super::*;

pub(crate) struct OwnedDnsRoot {
    pub(crate) tasks: OwnedDnsQueryTasks,
    #[cfg(feature = "udp")]
    udp: Arc<udp::OwnedDnsUdp>,
}
impl OwnedDnsRoot {
    pub(crate) fn build_backend(
        &self,
        server: &zero_config::DnsServerConfig,
        egress: zero_platform_tokio::EgressInterfaceControl,
    ) -> io::Result<ResolverBackend> {
        #[cfg(feature = "udp")]
        {
            if server.detour().is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "owned native UDP root cannot authorize a DNS detour",
                ));
            }
            ResolverBackend::build_owned_udp(server, egress, self.udp.clone())
        }
        #[cfg(not(feature = "udp"))]
        {
            let _ = (server, egress);
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "owned DNS UDP requires the udp feature",
            ))
        }
    }
}

#[cfg(feature = "udp")]
impl DnsSystem {
    /// Explicit native UDP library path. Uses the original host's private root
    /// grants and actual query Scope; it does not activate a production default.
    /// Other transports, system lookup, detours and truncated-UDP TCP fallback
    /// need their own actual native owners and are rejected by this UDP-only
    /// path before those unaccounted I/O operations. Legacy builders are intact.
    pub fn build_with_owned_udp(
        config: &DnsConfig,
        egress: zero_platform_tokio::EgressInterfaceControl,
        tasks: OwnedDnsQueryTasks,
        grants: Vec<zero_platform_tokio::contract::component::RootDatagramGrant>,
    ) -> io::Result<Self> {
        let root = Arc::new(OwnedDnsRoot {
            tasks,
            udp: udp::OwnedDnsUdp::new(grants)?,
        });
        Self::build_internal(
            Some(config),
            compile_standalone_dispatch(Some(config))?,
            egress,
            None,
            Some(root),
        )
    }
}

/// Owned UDP settles its actual original socket before returning the existing
/// backend deadline. Other transports retain their original outer timeout.
pub(crate) async fn exchange_backend_attempt(
    tag: &str,
    server: &ResolverServer,
    query: &[u8],
    snapshot: &ResolveSnapshot,
    next: Option<&ResolverBackend>,
    timeout_ms: u64,
) -> (io::Result<Vec<u8>>, bool) {
    let duration = std::time::Duration::from_millis(timeout_ms);
    let deadline = tokio::time::Instant::now() + duration;
    #[cfg(feature = "udp")]
    if let Some(resolver) = server.backend.owned_udp() {
        let (result, permits_fallback) = backends::ATTEMPT_DEADLINE
            .scope(deadline, resolver.exchange_owned(query))
            .await
            .into_parts(next);
        let result = result.and_then(|response| {
            if message::parse_response(query, &response)?.truncated {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "truncated owned DNS UDP requires a separately authorized native TCP root",
                ));
            }
            Ok(response)
        });
        return (result, permits_fallback);
    }
    #[cfg(not(feature = "udp"))]
    let _ = next;
    let result = tokio::time::timeout(
        duration,
        backends::ATTEMPT_DEADLINE.scope(
            deadline,
            server.backend.exchange(
                query,
                server.detour.as_deref(),
                snapshot.outbound_connector.as_deref(),
                snapshot.family,
            ),
        ),
    )
    .await
    .unwrap_or_else(|_| {
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            format!("DNS backend `{tag}` timed out after {timeout_ms}ms"),
        ))
    });
    (result, true)
}

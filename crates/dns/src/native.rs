//! The configured resolver borrows the actual host's current query owner.
use crate::OwnedDnsQueryTasks;
use std::{
    fmt, io,
    net::SocketAddr,
    sync::{Arc, RwLock, Weak},
};
use tokio::time::Instant;
use zero_platform_tokio::{
    contract::{scope::TaskJoinWitness, BoxFuture, CancellationToken},
    controlled_carrier::loan::{DatagramLoan, StreamLoan},
};
mod open_failure;
mod wire;
pub use open_failure::NativeDnsOpenFailure;
#[cfg(feature = "doq")]
pub use open_failure::NativeDnsQuicOpenFailure;
#[cfg(feature = "doq")]
pub use wire::native_quic_endpoints;
#[cfg(feature = "doh")]
pub(crate) use wire::Attempt;
pub use wire::{native_endpoints, validate_node_config, validate_node_server};
#[cfg(feature = "doq")]
pub struct NativeDnsQuicLoan {
    pub io: zero_platform_tokio::TcpRelayStream,
    pub owner: Box<dyn NativeDnsQuicControl>,
}
#[cfg(feature = "doq")]
pub trait NativeDnsQuicControl: Send {
    fn original(
        &self,
    ) -> &zero_platform_tokio::controlled_datagram::metadata_pool::MetadataPoolOrigin;
    fn close(
        &mut self,
        end: Instant,
    ) -> BoxFuture<
        '_,
        io::Result<zero_platform_tokio::controlled_datagram::metadata_pool::MetadataPoolClosed>,
    >;
}

/// Executable native I/O, not a Service label or a declaration-only capability.
pub trait NativeDnsProvider: Send + Sync {
    /// Invoked by the same real query/driver task after its I/O future drops.
    /// Stand-alone providers retain their explicit lifecycle; the actual kernel
    /// provider consumes its registered originals here, without another task.
    fn finish_query(&self, _caller: TaskJoinWitness) -> BoxFuture<'_, io::Result<()>> {
        Box::pin(async { Ok(()) })
    }
    /// Only after a selected, taskless Stream open has returned failure:
    /// consume its actual original controls and nested query joins. This is
    /// logical attempt settlement, never an invented native-close receipt.
    fn close_failed_stream_originals(
        &self,
        _caller: TaskJoinWitness,
        _end: Instant,
    ) -> BoxFuture<'_, io::Result<()>> {
        Box::pin(async {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "DNS failed Stream originals have no owner",
            ))
        })
    }
    /// Selection inspects only the actual executable graph, without advancing
    /// scheduling. Once selected, acquisition errors never retry a legacy path.
    fn owns_detour(&self, _outbound: &str) -> bool {
        false
    }
    fn detour_stream(
        &self,
        _outbound: String,
        _peer: SocketAddr,
        _caller: TaskJoinWitness,
        _cancel: CancellationToken,
        _end: Instant,
    ) -> BoxFuture<'_, io::Result<(StreamLoan, Arc<dyn crate::DnsCarrierBinding>)>> {
        Box::pin(async {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "DNS detour has no original provider",
            ))
        })
    }
    /// Called after the actual driver joins. The provider settles its retained
    /// opening originals, including an interrupted open before a loan returned.
    fn close_stream_originals(
        &self,
        _caller: TaskJoinWitness,
        _end: Instant,
    ) -> BoxFuture<'_, io::Result<()>> {
        Box::pin(async {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "DNS provider cannot confirm its retained stream originals",
            ))
        })
    }
    fn system(
        &self,
        name: String,
        caller: TaskJoinWitness,
        cancel: CancellationToken,
        end: Instant,
    ) -> BoxFuture<'_, io::Result<crate::NativeSystemLookup>> {
        Box::pin(async move { self.tasks().lookup_system(name, caller, cancel, end).await })
    }
    #[cfg(feature = "doq")]
    fn quic(
        &self,
        _peer: SocketAddr,
        _server_name: String,
        _client: quinn::ClientConfig,
        _caller: TaskJoinWitness,
        _cancel: CancellationToken,
        _end: Instant,
    ) -> BoxFuture<'_, io::Result<NativeDnsQuicLoan>> {
        Box::pin(async {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "native QUIC has no original provider",
            ))
        })
    }
    fn tasks(&self) -> OwnedDnsQueryTasks;
    fn stream(
        &self,
        peer: SocketAddr,
        caller: TaskJoinWitness,
        cancel: CancellationToken,
        end: Instant,
    ) -> BoxFuture<'_, io::Result<StreamLoan>>;
    fn datagram(
        &self,
        peer: SocketAddr,
        caller: TaskJoinWitness,
        cancel: CancellationToken,
        end: Instant,
    ) -> BoxFuture<'_, io::Result<DatagramLoan>>;
}
#[derive(Default)]
pub struct NativeDnsSource(RwLock<Option<Weak<dyn NativeDnsProvider>>>);
impl fmt::Debug for NativeDnsSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("NativeDnsSource")
    }
}
impl NativeDnsSource {
    pub(crate) fn owns_detour(&self, outbound: &str) -> bool {
        match self.provider() {
            Ok(provider) => provider.owns_detour(outbound),
            // A missing original host is a terminal admission failure, never
            // permission to run a submitted query through a naked connector.
            Err(_) => true,
        }
    }
    pub fn install(&self, provider: &Arc<dyn NativeDnsProvider>) -> io::Result<()> {
        let mut current = self.0.write().expect("DNS native source poisoned");
        if current.as_ref().is_some_and(|old| old.strong_count() != 0) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "previous native DNS owner is still installed",
            ));
        }
        provider.tasks().bind_finalizer(Arc::downgrade(provider))?;
        *current = Some(Arc::downgrade(provider));
        Ok(())
    }
    pub fn withdraw(&self, provider: &Arc<dyn NativeDnsProvider>) -> bool {
        let mut current = self.0.write().expect("DNS native source poisoned");
        if current
            .as_ref()
            .is_some_and(|old| Weak::ptr_eq(old, &Arc::downgrade(provider)))
        {
            current.take();
            true
        } else {
            false
        }
    }
    pub(crate) fn provider(&self) -> io::Result<Arc<dyn NativeDnsProvider>> {
        self.0
            .read()
            .expect("DNS native source poisoned")
            .as_ref()
            .and_then(Weak::upgrade)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotConnected,
                    "native DNS owner is not admitted by the host",
                )
            })
    }
    pub(crate) fn tasks(&self) -> io::Result<OwnedDnsQueryTasks> {
        Ok(self.provider()?.tasks())
    }
    pub(crate) async fn lookup_system(
        &self,
        name: String,
        end: Instant,
    ) -> io::Result<crate::NativeSystemLookup> {
        self.provider()?
            .system(
                name,
                OwnedDnsQueryTasks::current_witness()?,
                OwnedDnsQueryTasks::current_cancellation()?,
                end,
            )
            .await
    }
    pub(crate) fn exchange<'a>(
        &'a self,
        config: &'a zero_config::DnsServerConfig,
        backend: &'a crate::backends::ResolverBackend,
        query: &'a [u8],
        family: zero_traits::AddressFamily,
        end: Instant,
    ) -> BoxFuture<'a, wire::Attempt> {
        // Keep the optional native TLS/H2 state out of ordinary resolver and
        // forwarding futures, including when native registration is disabled.
        Box::pin(async move {
            let context = self.provider().and_then(|provider| {
                Ok((
                    provider,
                    OwnedDnsQueryTasks::current_witness()?,
                    OwnedDnsQueryTasks::current_cancellation()?,
                ))
            });
            let (provider, caller, cancel) = match context {
                Ok(context) => context,
                Err(source) => return wire::Attempt::blocked(source),
            };
            #[cfg(feature = "doh")]
            if let (
                zero_config::DnsServerConfig::Doh { .. },
                crate::backends::ResolverBackend::Doh(resolver),
            ) = (config, backend)
            {
                return resolver
                    .exchange_native(provider, config.detour(), query, end)
                    .await;
            }
            wire::exchange(
                provider.as_ref(),
                config,
                backend,
                query,
                family,
                caller,
                cancel,
                end,
            )
            .await
        })
    }
}

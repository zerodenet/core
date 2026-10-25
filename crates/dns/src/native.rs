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
mod wire;
pub use wire::native_endpoints;
#[cfg(feature = "doq")]
pub use wire::native_quic_endpoints;
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
    pub fn install(&self, provider: &Arc<dyn NativeDnsProvider>) -> io::Result<()> {
        let mut current = self.0.write().expect("DNS native source poisoned");
        if current.as_ref().is_some_and(|old| old.strong_count() != 0) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "previous native DNS owner is still installed",
            ));
        }
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
    pub(crate) fn exchange<'a>(
        &'a self,
        config: &'a zero_config::DnsServerConfig,
        backend: &'a crate::backends::ResolverBackend,
        query: &'a [u8],
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
            wire::exchange(
                provider.as_ref(),
                config,
                backend,
                query,
                caller,
                cancel,
                end,
            )
            .await
        })
    }
}

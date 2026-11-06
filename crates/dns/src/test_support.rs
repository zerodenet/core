//! Explicit feature-gated fixtures, not a production/user TLS configuration API.
//! These helpers never construct a resolver, socket, pool, task, or fake receipt.
use crate::{backends::ResolverBackend, DnsSystem, DnsSystemInner};
use std::{io, sync::Arc};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DohPoolWitness {
    /// Actual Scope/standalone driver registrations, including unjoined exits.
    pub registered_driver_tasks: usize,
    /// Actual admitted H2 users, including users of retired joined drivers.
    pub active_leases: usize,
    pub closing: bool,
}
/// Trust this fixture CA while retaining real TLS hostname/certificate checks.
/// Only an already configured DoH backend that has never started I/O qualifies.
/// A snapshot borrower or any previous open attempt prevents mutation.
pub fn install_doh_ca(
    dns: &DnsSystem,
    tag: &str,
    certificate: rustls::pki_types::CertificateDer<'static>,
) -> io::Result<()> {
    let mut inner = dns.inner.write().expect("DNS system lock poisoned");
    let DnsSystemInner::Configured { servers, .. } = &mut *inner else {
        return Err(invalid());
    };
    let server = servers.get_mut(tag).ok_or_else(invalid)?;
    let backend = Arc::get_mut(&mut server.backend).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::WouldBlock,
            "a real DNS snapshot already borrows this backend",
        )
    })?;
    match backend {
        ResolverBackend::Doh(resolver) => resolver.install_test_ca(certificate),
        _ => Err(invalid()),
    }
}
/// Pure observation: does not reap/join tasks, release leases, retire resources,
/// poll the driver, or advance a selector/load-balance decision.
pub fn doh_pool_witness(dns: &DnsSystem, tag: &str) -> io::Result<DohPoolWitness> {
    let inner = dns.inner.read().expect("DNS system lock poisoned");
    let DnsSystemInner::Configured { servers, .. } = &*inner else {
        return Err(invalid());
    };
    match servers.get(tag).map(|server| server.backend.as_ref()) {
        Some(ResolverBackend::Doh(resolver)) => Ok(resolver.owner.test_witness()),
        _ => Err(invalid()),
    }
}
fn invalid() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "fixture requires an actual configured DoH server",
    )
}
#[cfg(feature = "doq")]
pub fn install_doq_ca(
    dns: &DnsSystem,
    tag: &str,
    certificate: rustls::pki_types::CertificateDer<'static>,
) -> io::Result<()> {
    let mut inner = dns.inner.write().expect("DNS system lock poisoned");
    let DnsSystemInner::Configured { servers, .. } = &mut *inner else {
        return Err(invalid());
    };
    let server = servers.get_mut(tag).ok_or_else(invalid)?;
    let backend = Arc::get_mut(&mut server.backend).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::WouldBlock,
            "a DNS snapshot borrows this backend",
        )
    })?;
    match backend {
        ResolverBackend::Doq(resolver) => resolver.install_test_ca(certificate),
        _ => Err(invalid()),
    }
}
/// Register a real failing job in the current original native query Scope.
/// No I/O, provider, driver or successful close evidence is fabricated here.
pub fn panic_native_query_worker(
    dns: &DnsSystem,
) -> io::Result<zero_platform_tokio::contract::scope::TaskJoinWitness> {
    let mut candidates = dns
        .native_source
        .as_ref()
        .into_iter()
        .chain(dns.query_coordinator.native_source())
        .chain(dns.node_queries.as_ref().map(|node| &node.source))
        .filter_map(|source| source.tasks().ok())
        .collect::<Vec<_>>();
    let index = candidates
        .iter()
        .position(|tasks| {
            tasks
                .observe()
                .iter()
                .any(|query| query.io_pending && !query.task.is_joined())
        })
        .unwrap_or(0);
    if candidates.is_empty() {
        return Err(invalid());
    }
    let tasks = candidates.swap_remove(index);
    let before = tasks.observe();
    tasks.spawn(
        async { panic!("original native DNS query worker failure") },
        zero_platform_tokio::contract::CancellationToken::new(),
    )?;
    tasks
        .observe()
        .into_iter()
        .find(|current| {
            !before
                .iter()
                .any(|old| old.task.same_registration(&current.task))
        })
        .map(|query| query.task)
        .ok_or_else(invalid)
}
#[cfg(feature = "dot")]
pub fn install_dot_ca(
    dns: &DnsSystem,
    tag: &str,
    certificate: rustls::pki_types::CertificateDer<'static>,
) -> io::Result<()> {
    let mut inner = dns.inner.write().expect("DNS system lock poisoned");
    let DnsSystemInner::Configured { servers, .. } = &mut *inner else {
        return Err(invalid());
    };
    let server = servers.get_mut(tag).ok_or_else(invalid)?;
    let backend = Arc::get_mut(&mut server.backend).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::WouldBlock,
            "a DNS snapshot borrows the backend",
        )
    })?;
    match backend {
        ResolverBackend::Dot(resolver) => resolver.install_test_ca(certificate),
        _ => Err(invalid()),
    }
}

mod detour;
pub use detour::{open_native_detour, NativeDetourStream};

/// Fail the existing actual DoH driver, never register a substitute query/task.
#[cfg(feature = "doh")]
pub fn panic_original_doh_driver(
    dns: &DnsSystem,
    tag: &str,
) -> io::Result<zero_platform_tokio::contract::scope::TaskJoinWitness> {
    let owner = {
        let inner = dns.inner.read().expect("DNS system lock poisoned");
        let DnsSystemInner::Configured { servers, .. } = &*inner else {
            return Err(invalid());
        };
        let server = servers.get(tag).ok_or_else(invalid)?;
        let ResolverBackend::Doh(resolver) = server.backend.as_ref() else {
            return Err(invalid());
        };
        resolver.owner.clone()
    };
    owner.panic_original_driver()
}

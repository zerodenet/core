//! Explicit feature-gated fixtures, not a production/user TLS configuration API.
//! These helpers never construct a resolver, socket, pool, task, or fake receipt.
use crate::{backends::ResolverBackend, DnsSystem, DnsSystemInner};
use std::{io, sync::Arc};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DohPoolWitness {
    /// Actual JoinSet members, including completed tasks not yet joined.
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

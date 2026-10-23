//! Public resolver contract backed by the actual configured DNS state.
use crate::{message, DnsSystem};
use std::{io, sync::atomic::Ordering};
use tokio::time::Instant;
use zero_platform_tokio::contract::{
    context::with_deadline,
    dns::{DnsGeneration, DnsQueryContext, DnsRequest, DnsResolution, DnsService},
    BoxFuture, ChannelError, DeliveryState, Impact, Reason, Stage,
};

impl DnsService for DnsSystem {
    fn generation(&self) -> DnsGeneration {
        // Resolver publication increments its generation under this same lock.
        let _inner = self.inner.read().expect("DNS system lock poisoned");
        DnsGeneration::new(
            self.config_generation.load(Ordering::Acquire),
            self.egress_interface.generation(),
        )
    }
    fn resolve(
        &self,
        request: DnsRequest,
        context: DnsQueryContext,
    ) -> BoxFuture<'_, Result<DnsResolution, ChannelError>> {
        Box::pin(async move {
            let name = message::normalize_domain(&request.name)
                .map_err(|source| failure(Reason::InvalidConfig, false).with_source(source))?;
            if context.cancel.is_cancelled() {
                return Err(failure(Reason::Cancelled, false));
            }
            if context
                .deadline
                .is_some_and(|deadline| Instant::now() >= deadline)
            {
                return Err(failure(Reason::Timeout, false));
            }
            let generation = self.generation();
            self.query_coordinator
                .ensure_open()
                .map_err(|source| failure(Reason::Revoked, false).with_source(source))?;
            if context
                .expected_generation
                .is_some_and(|expected| expected != generation)
            {
                return Err(failure(Reason::StalePlan, false));
            }
            let work = self.resolve_real_for_role_with_family(&name, request.role, request.family);
            let addresses = tokio::select! { biased;
                _ = context.cancel.cancelled() => return Err(failure(Reason::Cancelled,true)),
                result = with_deadline(context.deadline,work) => result
                    .map_err(|source|failure(Reason::Timeout,true).with_source(source))?
                    .map_err(|source|failure(match source.kind() {
                        io::ErrorKind::TimedOut=>Reason::Timeout,
                        io::ErrorKind::InvalidInput=>Reason::InvalidConfig,
                        io::ErrorKind::Interrupted=>Reason::Cancelled,
                        _=>Reason::Io,
                    },true).with_source(source))?,
            };
            if context.cancel.is_cancelled() {
                return Err(failure(Reason::Cancelled, true));
            }
            if self.generation() != generation {
                return Err(failure(Reason::StalePlan, true));
            }
            Ok(DnsResolution {
                addresses,
                generation,
            })
        })
    }
}

impl DnsSystem {
    /// Close every actual DoQ endpoint retained by successful or cancelled
    /// queries, await QUIC idle, drop endpoint handles, and join both drivers.
    /// Call after shutdown_queries; deadlines retain the same owner for retry.
    pub async fn shutdown_doq_endpoints(&self, deadline: Instant) -> io::Result<()> {
        self.transport_owners.shutdown_doq(deadline).await
    }

    /// Close only persistent DoH pools and join their actual H2 drivers/users,
    /// including owners retained across reload. Call after shutdown_queries.
    /// DoQ endpoint idle confirmation and Fake-IP persistence are separate.
    pub async fn shutdown_doh_pools(&self, deadline: Instant) -> io::Result<()> {
        self.transport_owners.shutdown_doh(deadline).await
    }

    /// Stop query admission and join both real coordinator worker sets. A
    /// timeout retains ownership for another confirmation attempt. Resolver
    /// pools/Fake-IP persistence have separate resource lifecycle obligations.
    pub async fn shutdown_queries(&self, deadline: Instant) -> io::Result<()> {
        let (names, wire) = tokio::join!(
            self.query_coordinator.shutdown(deadline),
            self.wire_query_coordinator.shutdown(deadline)
        );
        names.and(wire)
    }
}

fn failure(reason: Reason, started: bool) -> ChannelError {
    let mut error = ChannelError::new(Stage::Prepare, reason, Impact::Session);
    if !started {
        error.delivery = DeliveryState::NotSubmitted;
    }
    error
}

#[cfg(test)]
mod tests;

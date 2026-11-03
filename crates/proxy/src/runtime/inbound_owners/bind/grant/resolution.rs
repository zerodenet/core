//! The actual configuration owner retains and joins the real system lookup.
use super::*;
use crate::component::instance::retained;
use std::net::ToSocketAddrs;
use zero_core::component::TaskId;
use zero_platform_tokio::contract::scope::{TaskClass, TaskJoinWitness, TaskRegistrar};

impl NativeTcpGrant {
    pub(in crate::runtime::inbound_owners::bind) fn needs_resolution(&self) -> bool {
        matches!(self.destination.address, zero_core::Address::Domain(_))
    }

    pub(in crate::runtime::inbound_owners::bind) fn start_resolution(
        self: &Arc<Self>,
        registrar: &TaskRegistrar,
    ) -> Result<(TaskId, TaskJoinWitness), EngineError> {
        let zero_core::Address::Domain(host) = &self.destination.address else {
            return Err(self.error(
                Reason::InvalidConfig,
                "only a hostname listener has a lookup task",
            ));
        };
        let host = host.clone();
        let port = self.destination.port;
        let limit = self.resolution_limit;
        self.start_lookup(registrar, move || {
            let mut addresses = Vec::new();
            for address in (host.as_str(), port).to_socket_addrs()? {
                if !addresses.contains(&address) {
                    addresses.push(address);
                }
                if addresses.len() == limit {
                    break;
                }
            }
            if addresses.is_empty() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::AddrNotAvailable,
                    "listener hostname resolved to no addresses",
                ));
            }
            Ok(addresses.into())
        })
    }

    // Concrete work is submitted once, not retained as another provider SPI.
    pub(in crate::runtime::inbound_owners::bind) fn start_lookup(
        self: &Arc<Self>,
        registrar: &TaskRegistrar,
        lookup: impl FnOnce() -> std::io::Result<Arc<[SocketAddr]>> + Send + 'static,
    ) -> Result<(TaskId, TaskJoinWitness), EngineError> {
        if let Some(task) = self.resolution.get() {
            return Ok(task.clone());
        }
        if self.cancel.is_cancelled() {
            return Err(super::super::super::preparing::stopped_preparation());
        }
        let grant = self.clone();
        let task = registrar
            .spawn_tracked(TaskClass::Critical, async move {
                // Cancellation of the configuration borrower does not cancel this
                // real Scope task or detach its blocking worker. Cleanup must join.
                let result = match tokio::task::spawn_blocking(lookup).await {
                    Ok(result) => result.map_err(|source| {
                        ChannelError::new(Stage::Prepare, Reason::Io, Impact::Component)
                            .with_source(source)
                    }),
                    Err(source) => {
                        Err(
                            ChannelError::new(Stage::Prepare, Reason::Internal, Impact::Component)
                                .with_source(source),
                        )
                    }
                };
                let result = match result {
                    Ok(addresses) if !grant.cancel.is_cancelled() => {
                        let installed = grant
                            .native
                            .lock()
                            .await
                            .install_resolved_addresses(addresses.clone());
                        installed.map(|()| {
                            grant
                                .addresses
                                .set(addresses.clone())
                                .expect("one actual listener resolution");
                            addresses
                        })
                    }
                    other => other,
                }
                .map_err(Arc::new);
                let failure = result.as_ref().err().cloned();
                assert!(
                    grant.resolved.set(result).is_ok(),
                    "one original lookup result"
                );
                failure.map_or(Ok(()), |source| Err(retained(source)))
            })
            .map_err(std::io::Error::other)?;
        assert!(
            self.resolution.set(task.clone()).is_ok(),
            "one original lookup task"
        );
        Ok(task)
    }
    pub(in crate::runtime::inbound_owners::bind) fn resolution_result(
        &self,
    ) -> Result<(), EngineError> {
        match self.resolved.get() {
            Some(Err(source)) => Err(std::io::Error::other(retained(source.clone())).into()),
            Some(Ok(_)) if !self.cancel.is_cancelled() => Ok(()),
            _ => Err(super::super::super::preparing::stopped_preparation()),
        }
    }
}

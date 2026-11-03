//! The same configuration Scope retains the real lookup before native bind.
use super::*;
use crate::component::instance::retained;
use std::net::ToSocketAddrs;
use zero_platform_tokio::contract::scope::{TaskClass, TaskJoinWitness, TaskRegistrar};
impl NativeDatagramGrant {
    pub(in crate::runtime::inbound_owners::bind) fn needs_resolution(&self) -> bool {
        matches!(&self.declaration, ResourceDeclaration::DatagramListener { destination, .. } if matches!(destination.address, zero_core::Address::Domain(_)))
    }
    pub(in crate::runtime::inbound_owners::bind) fn start_resolution(
        self: &Arc<Self>,
        registrar: &TaskRegistrar,
    ) -> Result<(zero_core::component::TaskId, TaskJoinWitness), EngineError> {
        if let Some(task) = self.resolution.get() {
            return Ok(task.clone());
        }
        let ResourceDeclaration::DatagramListener {
            destination,
            max_bind_attempts,
            ..
        } = &self.declaration
        else {
            unreachable!()
        };
        let zero_core::Address::Domain(host) = &destination.address else {
            unreachable!()
        };
        let host = host.clone();
        let port = destination.port;
        let limit = max_bind_attempts.get();
        let grant = self.clone();
        let task = registrar
            .spawn_tracked(TaskClass::Critical, async move {
                let lookup = tokio::task::spawn_blocking(move || {
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
                            "UDP hostname resolved to no addresses",
                        ));
                    }
                    Ok(Arc::<[std::net::SocketAddr]>::from(addresses))
                })
                .await;
                let result: Result<(), ChannelError> = (|| {
                    let addresses = lookup
                        .map_err(|e| {
                            ChannelError::new(Stage::Prepare, Reason::Internal, Impact::Component)
                                .with_source(e)
                        })?
                        .map_err(|e| {
                            ChannelError::new(Stage::Prepare, Reason::Io, Impact::Component)
                                .with_source(e)
                        })?;
                    if grant.cancel.is_cancelled() {
                        return Err(ChannelError::new(
                            Stage::Prepare,
                            Reason::Revoked,
                            Impact::Component,
                        ));
                    }
                    let original = HostDatagramListenerOwner::new_resolved(
                        grant.identity.component,
                        &grant.declaration,
                        grant.attempts,
                        addresses.clone(),
                    )?;
                    let mut native = grant.native.lock().unwrap();
                    if grant.cancel.is_cancelled() {
                        return Err(ChannelError::new(
                            Stage::Prepare,
                            Reason::Revoked,
                            Impact::Component,
                        ));
                    }
                    assert!(native.is_none(), "one original native UDP owner");
                    *native = Some(original);
                    let _ = grant.addresses.set(addresses);
                    Ok(())
                })();
                let result = result.map_err(Arc::new);
                let failure = result.as_ref().err().cloned();
                assert!(
                    grant.resolved.set(result).is_ok(),
                    "one original UDP lookup"
                );
                failure.map_or(Ok(()), |source| Err(retained(source)))
            })
            .map_err(std::io::Error::other)?;
        assert!(
            self.resolution.set(task.clone()).is_ok(),
            "one original lookup registration"
        );
        Ok(task)
    }
    pub(in crate::runtime::inbound_owners::bind) fn resolution_result(
        &self,
    ) -> Result<(), EngineError> {
        match self.resolved.get() {
            Some(Ok(())) if !self.cancel.is_cancelled() => Ok(()),
            Some(Err(source)) => Err(std::io::Error::other(retained(source.clone())).into()),
            _ => Err(crate::runtime::inbound_owners::preparing::stopped_preparation()),
        }
    }
}

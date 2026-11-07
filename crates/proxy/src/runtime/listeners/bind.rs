//! Temporary capability bridge; one persistent reservation precedes native I/O.
use std::{num::NonZeroUsize, path::Path, time::Duration};

use tokio::sync::watch;
use zero_config::InboundConfig;
use zero_engine::EngineError;

use crate::{
    inventory::ProtocolInventory,
    runtime::{inbound_owners::LeasedInbound, route_runtime::InboundListenerRuntimeFactory},
};

pub(in crate::runtime) enum PendingInbound {
    Leased(LeasedInbound),
}
impl PendingInbound {
    pub(super) fn shutdown_sender(&self, _legacy: &watch::Sender<bool>) -> watch::Sender<bool> {
        match self {
            Self::Leased(lease) => lease.shutdown_sender(),
        }
    }
}

pub(super) async fn bind_listener(
    protocols: &ProtocolInventory,
    runtime_factory: &InboundListenerRuntimeFactory,
    source_dir: Option<&Path>,
    inbound: &InboundConfig,
    shutdown: watch::Sender<bool>,
    attempts: NonZeroUsize,
) -> Result<PendingInbound, EngineError> {
    let prepared = protocols.prepare_inbound_bind_in_config(
        runtime_factory.listener_config(inbound),
        &runtime_factory.config_handle(),
    )?;
    bind_prepared_listener(
        protocols,
        runtime_factory,
        source_dir,
        inbound,
        shutdown,
        attempts,
        prepared,
    )
    .await
}

/// Consume the same cold operation only after every affected old owner closed.
/// None is an explicit legacy shape, never a retry through a different capability.
pub(super) async fn bind_prepared_listener(
    protocols: &ProtocolInventory,
    runtime_factory: &InboundListenerRuntimeFactory,
    source_dir: Option<&Path>,
    inbound: &InboundConfig,
    shutdown: watch::Sender<bool>,
    attempts: NonZeroUsize,
    prepared: Option<crate::protocol_registry::PreparedInboundBind>,
) -> Result<PendingInbound, EngineError> {
    let _ = source_dir;
    if let Some(prepared) = prepared {
        let mut lease = runtime_factory.owners().reserve_listener(
            &inbound.tag,
            &runtime_factory.config_handle(),
            protocols,
            prepared,
            shutdown,
            attempts,
        )?;
        if let Err(error) = runtime_factory.register_preparing(&lease.owner()) {
            return Err(lease.reject(error));
        }
        if let Err(error) = lease.resolve_native().await {
            return Err(lease.reject(error));
        }
        for attempt in 0..attempts.get() {
            match lease.bind_native() {
                Ok(()) => return lease.finish().map(PendingInbound::Leased),
                Err(error) => {
                    // Only the actual native error can prove a safe retry.
                    // Unknown, erased metadata and exhausted budgets stop here.
                    let retry = crate::transport::channel_failure(&error)
                        .is_some_and(|error| error.retry_permitted());
                    if !retry || attempt + 1 == attempts.get() {
                        return Err(lease.reject(error));
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        unreachable!("nonzero actual bind-attempt budget")
    }

    Err(std::io::Error::other("inbound has no registered standard component").into())
}

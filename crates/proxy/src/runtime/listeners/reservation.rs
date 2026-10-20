//! Socket reservation precedes live listener mutation during configuration apply.
use std::{collections::HashMap, path::Path};

use zero_config::{InboundConfig, RuntimeConfig};
use zero_engine::EngineError;

use super::bind::PendingInbound;
use crate::{inventory::ProtocolInventory, runtime::route_runtime::InboundListenerRuntimeFactory};

pub(super) async fn reserve_candidates(
    protocols: &ProtocolInventory,
    runtime_factory: &InboundListenerRuntimeFactory,
    source_dir: Option<&Path>,
    config: &RuntimeConfig,
    active: &HashMap<String, InboundConfig>,
) -> Result<HashMap<String, PendingInbound>, EngineError> {
    let mut reserved = HashMap::new();
    for inbound in &config.inbounds {
        if !runtime_factory
            .endpoint_admission()
            .listener_enabled(&inbound.tag)
        {
            continue;
        }
        if active
            .get(&inbound.tag)
            .is_none_or(|current| current.listen.port != inbound.listen.port)
            && !active
                .values()
                .any(|current| current.listen.port == inbound.listen.port)
        {
            let (shutdown, _) = tokio::sync::watch::channel(false);
            let bound =
                bind_inbound_with_retry(protocols, runtime_factory, source_dir, inbound, shutdown)
                    .await?;
            reserved.insert(inbound.tag.clone(), bound);
        }
    }
    // Same-port replacements, including a new tag reusing a removed tag's
    // port, still use stop/rebind and rollback. Conservatively skip reservation
    // whenever a live listener uses that port, even for distinct transports.
    Ok(reserved)
}

pub(super) async fn bind_inbound_with_retry(
    protocols: &ProtocolInventory,
    runtime_factory: &InboundListenerRuntimeFactory,
    source_dir: Option<&Path>,
    inbound: &InboundConfig,
    shutdown: tokio::sync::watch::Sender<bool>,
) -> Result<PendingInbound, EngineError> {
    super::bind::bind_listener(
        protocols,
        runtime_factory,
        source_dir,
        inbound,
        shutdown,
        std::num::NonZeroUsize::new(50).unwrap(),
    )
    .await
}

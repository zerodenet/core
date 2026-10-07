//! Socket reservation precedes live listener mutation during configuration apply.
use std::{collections::HashMap, path::Path, time::Duration};

use zero_config::{InboundConfig, RuntimeConfig};
use zero_engine::EngineError;

use super::inbound::bind_inbound_listener;
use crate::{
    inventory::ProtocolInventory, protocol_registry::BoundInbound,
    runtime::route_runtime::InboundListenerRuntimeFactory,
};

pub(super) async fn reserve_candidates(
    protocols: &ProtocolInventory,
    runtime_factory: &InboundListenerRuntimeFactory,
    source_dir: Option<&Path>,
    config: &RuntimeConfig,
    active: &HashMap<String, InboundConfig>,
) -> Result<HashMap<String, BoundInbound>, EngineError> {
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
            let bound =
                bind_inbound_with_retry(protocols, runtime_factory, source_dir, inbound).await?;
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
) -> Result<BoundInbound, EngineError> {
    let mut last_error = None;
    for attempt in 0..50 {
        match bind_inbound_listener(protocols, runtime_factory, source_dir, inbound).await {
            Ok(bound) => return Ok(bound),
            Err(error) => last_error = Some(error),
        }
        if attempt < 49 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    Err(last_error.expect("bind retry loop always attempts at least once"))
}

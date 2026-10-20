//! Retry entry for acquisitions after the actual old listener retirement barrier.
use std::path::Path;

use zero_config::InboundConfig;
use zero_engine::EngineError;

use super::bind::PendingInbound;
use crate::{inventory::ProtocolInventory, runtime::route_runtime::InboundListenerRuntimeFactory};

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

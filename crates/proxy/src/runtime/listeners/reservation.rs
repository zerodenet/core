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
    let prepared =
        match runtime_factory
            .owners()
            .prepare_recovery_bind(inbound.clone(), source_dir, true)?
        {
            Some(prepared) => Some(prepared),
            None => protocols
                .prepare_inbound_bind(runtime_factory.listener_config(inbound), source_dir)?,
        };
    super::bind::bind_prepared_listener(
        protocols,
        runtime_factory,
        source_dir,
        inbound,
        shutdown,
        std::num::NonZeroUsize::new(50).unwrap(),
        prepared,
    )
    .await
}

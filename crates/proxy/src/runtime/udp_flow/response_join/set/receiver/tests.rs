//! Actual Host books and original hosted tasks; packet work creates no task.
use super::*;
use std::{future::poll_fn, num::NonZeroUsize, task::Poll, time::Duration};
use tokio::{
    net::UdpSocket,
    sync::{mpsc, oneshot},
};
use zero_core::component::AssociationId;
use zero_platform_tokio::contract::{
    scope::{TaskClass, TaskScope},
    CancellationToken,
};
#[derive(Debug, Default)]
struct Sources(std::sync::OnceLock<Arc<std::io::Error>>);
impl std::fmt::Display for Sources {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("actual receiver source")
    }
}
impl std::error::Error for Sources {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.get().map(|source| source.as_ref() as _)
    }
}
fn memory() -> (crate::runtime::Proxy, BusinessMemory) {
    let proxy = crate::runtime::Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap();
    let host = proxy.current_service_source().upgrade().unwrap();
    let (bytes, metadata) = host.packet_path_memory().unwrap();
    (proxy, BusinessMemory { bytes, metadata })
}
fn sources(memory: &BusinessMemory) -> PacketMetadataArc<Sources> {
    let credit = memory
        .metadata
        .reserve(PacketMetadataArc::<Sources>::layout().unwrap())
        .unwrap();
    PacketMetadataArc::from_prepaid(Sources::default(), credit)
        .unwrap_or_else(|_| panic!("exact sources"))
}
fn end() -> Instant {
    Instant::now() + Duration::from_secs(3)
}
mod ordinary;
mod panic;

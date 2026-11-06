use std::sync::{atomic::AtomicU64, Arc};
use zero_stack::UserUdpStack;

pub(super) async fn run(
    proxy: crate::Proxy,
    stack: Arc<UserUdpStack>,
    tag: String,
    dns_hijack: bool,
    dns_hijacked_queries: Arc<AtomicU64>,
) -> Result<(), zero_engine::EngineError> {
    let services = proxy.tcp_runtime_services().for_inbound(tag.clone());
    // Explicit standalone stack fixture; production requires owned Source tasks.
    super::super::run_inner(
        crate::runtime::udp_ingress::UdpIngressRuntime::new(services),
        stack,
        tag,
        dns_hijack,
        dns_hijacked_queries,
        #[cfg(feature = "raw-ip-runtime")]
        None,
    )
    .await
}

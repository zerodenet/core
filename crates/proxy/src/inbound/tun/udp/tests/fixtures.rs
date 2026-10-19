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
    super::super::run_with_runtime(
        crate::runtime::udp_ingress::UdpIngressRuntime::new(services),
        stack,
        tag,
        dns_hijack,
        dns_hijacked_queries,
    )
    .await
}

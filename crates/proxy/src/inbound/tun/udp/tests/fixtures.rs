use super::super::{OwnedTunUdpTasks, UdpScratch};
pub(super) use crate::runtime::inbound_operation::raw_ip::udp_input::start as run;
use std::sync::{atomic::AtomicU64, Arc};

pub(crate) fn run_owned(
    runtime: crate::runtime::udp_ingress::UdpIngressRuntime,
    stack: Arc<zero_stack::UserUdpStack>,
    tag: String,
    tasks: OwnedTunUdpTasks,
    scratch: UdpScratch,
    dns_hijack: bool,
    dns_hijacked_queries: Arc<AtomicU64>,
) -> impl std::future::Future<Output = Result<(), zero_engine::EngineError>> + Send + 'static {
    super::super::run_inner(
        runtime,
        stack,
        tag,
        dns_hijack,
        dns_hijacked_queries,
        Some(tasks),
        Some(scratch),
    )
}

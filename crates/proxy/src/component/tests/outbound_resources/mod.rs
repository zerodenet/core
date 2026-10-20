use super::*;
use std::{num::NonZeroUsize, time::Duration};
use tokio::time::{timeout, Instant};
use zero_core::component::{ComponentId, ResourceGeneration};
use zero_platform_tokio::contract::{lifecycle::TaskClass, scope::TaskScope};

mod access;
mod failures;

fn instance() -> ComponentInstanceId {
    ComponentInstanceId {
        component: ComponentId(301),
        generation: ResourceGeneration(7),
    }
}
fn declaration(peer: SocketAddr, control: &EgressInterfaceControl) -> ResourceDeclaration {
    ResourceDeclaration::OutboundDatagram {
        peer,
        network_generation: NetworkGeneration(control.generation()),
        max_sockets: NonZeroUsize::new(1).unwrap(),
    }
}
fn request(peer: SocketAddr, control: &EgressInterfaceControl) -> OutboundDatagramRequest {
    OutboundDatagramRequest {
        peer,
        expected_network_generation: NetworkGeneration(control.generation()),
    }
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}
fn binding(runtime: tokio::runtime::Handle) -> (TaskScope, TaskId) {
    let scope = TaskScope::new(runtime, NonZeroUsize::new(1).unwrap());
    let cancel = scope.registrar().cancellation();
    let task = scope
        .registrar()
        .spawn(TaskClass::Critical, async move {
            cancel.cancelled().await;
            Ok(())
        })
        .unwrap();
    (scope, task)
}
fn provider(
    peer: SocketAddr,
    control: &EgressInterfaceControl,
    cancel: CancellationToken,
) -> Arc<HostOutboundDatagramResources> {
    HostOutboundDatagramResources::new(
        instance(),
        &declaration(peer, control),
        control.clone(),
        cancel,
    )
    .unwrap()
}

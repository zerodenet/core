//! Actual suspended Host/registered driver, no native UDP stand-in.
use super::*;
use std::num::NonZeroUsize;
use tokio::time::{Duration, Instant};
use zero_core::component::{ComponentInstanceId, ResourceGeneration};
use zero_platform_tokio::contract::{
    component::{PacketSupplementalMemory, ResourceAccess},
    lifecycle::TaskClass,
    scope::{HostedScope, TaskScope},
};
mod issuance;
mod pressure;
fn nz(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}
fn end() -> Instant {
    Instant::now() + Duration::from_secs(3)
}
fn instance(id: u64) -> ComponentInstanceId {
    ComponentInstanceId {
        component: ComponentId(id),
        generation: ResourceGeneration(1),
    }
}
fn declaration() -> ResourceDeclaration {
    ResourceDeclaration::LogicalPacketWorkspaces {
        workspaces: [nz(64); 2],
        control_workspace: Some(nz(16)),
        supplemental: PacketSupplementalMemory {
            receive_workspaces: vec![nz(32)],
            transmit_copy: Some(nz(64)),
            transmit_target: Some(nz(255)),
        },
    }
}
fn pending(scope: &HostedScope) -> (tokio::sync::oneshot::Sender<()>, TaskJoinWitness) {
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let (_, witness) = scope
        .registrar()
        .spawn_tracked(TaskClass::Auxiliary, async move {
            let _ = stopped.await;
            Ok(())
        })
        .unwrap();
    (stop, witness)
}
async fn finish(
    scope: &mut HostedScope,
    stop: tokio::sync::oneshot::Sender<()>,
    witness: &TaskJoinWitness,
) {
    stop.send(()).unwrap();
    let receipt = tokio::time::timeout_at(end(), scope.next_receipt())
        .await
        .unwrap()
        .unwrap();
    assert!(witness.matches_joined(receipt.joined()));
    receipt.result().unwrap();
    scope.acknowledge(&receipt).unwrap();
    assert!(witness
        .wait_registration_released()
        .await
        .registration_released());
}

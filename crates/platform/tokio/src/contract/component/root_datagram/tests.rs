use super::*;
use crate::contract::{
    scope::{TaskClass, TaskScope},
    BoxFuture,
};
use std::{num::NonZeroUsize, path::PathBuf};
use zero_core::component::{ComponentId, ResourceGeneration};
use zero_traits::DialPolicy;
struct Deny;
impl ResourceAccess for Deny {
    fn read_file(&self, _: PathBuf, _: usize) -> BoxFuture<'_, Result<Vec<u8>, ChannelError>> {
        Box::pin(async { Err(denied()) })
    }
    fn listen_tcp(
        &self,
        _: zero_core::request::Destination,
    ) -> BoxFuture<'_, Result<crate::ControlledTcpListener, ChannelError>> {
        Box::pin(async { Err(denied()) })
    }
    fn listen_datagram(
        &self,
        _: zero_core::request::Destination,
    ) -> BoxFuture<'_, Result<super::super::GrantedDatagramListener, ChannelError>> {
        Box::pin(async { Err(denied()) })
    }
    fn open_device(
        &self,
        _: Arc<str>,
    ) -> BoxFuture<'_, Result<crate::contract::packet::BoxPacketSession, ChannelError>> {
        Box::pin(async { Err(denied()) })
    }
}
fn grant(
    authority: &RootDatagramAuthority,
    access: &Arc<dyn ResourceAccess>,
    ordinal: usize,
) -> RootDatagramGrant {
    authority.grant(
        Arc::downgrade(access),
        ComponentInstanceId {
            component: ComponentId(910),
            generation: ResourceGeneration(1),
        },
        ordinal,
        "127.0.0.1:9".parse().unwrap(),
        NetworkGeneration(0),
    )
}
fn query(scope: &TaskScope) -> (TaskJoinWitness, tokio::sync::oneshot::Sender<()>) {
    let (done, wait) = tokio::sync::oneshot::channel();
    let (_, witness) = scope
        .registrar()
        .spawn_tracked(TaskClass::Auxiliary, async move {
            let _ = wait.await;
            Ok(())
        })
        .unwrap();
    (witness, done)
}
#[tokio::test]
async fn retry_requires_actual_settled_owner_same_private_root_and_same_unjoined_query() {
    let mut scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(2).unwrap(),
    );
    scope.enable_registration().unwrap();
    let (witness, done) = query(&scope);
    let (other_query, other_done) = query(&scope);
    let authority = RootDatagramAuthority::issue();
    let access: Arc<dyn ResourceAccess> = Arc::new(Deny);
    let first = grant(&authority, &access, 0);
    let next = grant(&authority, &access, 1);
    let foreign = grant(&RootDatagramAuthority::issue(), &access, 1);
    let peer = first.peer();
    let selection = crate::EgressInterfaceControl::default()
        .select_for_peer_with_policy(peer, &DialPolicy::default())
        .unwrap();
    let invalid = DialPolicy {
        source_ip: Some("192.0.2.197".parse().unwrap()),
        ..Default::default()
    };
    let mut owner = ControlledDatagramOwner::new(NonZeroUsize::new(1).unwrap());
    let acquisition = owner.acquire(peer, None, &invalid, &selection).unwrap_err();
    let original = acquisition.source.clone();
    assert!(owner.confirms_settled_acquisition(&acquisition));
    let failure =
        authority.acquisition_failure(&first.request, &witness, denied(), acquisition, &owner);
    let receipt = failure.retry_receipt().unwrap();
    assert!(receipt.permits(&next, &witness));
    assert!(!receipt.permits(&foreign, &witness));
    assert!(!receipt.permits(&next, &other_query));
    use std::error::Error;
    let retained = failure
        .error()
        .source()
        .unwrap()
        .downcast_ref::<DatagramAcquisitionError>()
        .unwrap();
    assert!(Arc::ptr_eq(&retained.source, &original));
    assert!(
        first
            .open(peer, first.network_generation(), &witness)
            .err()
            .unwrap()
            .retry_receipt()
            .is_none(),
        "default-deny diagnostic cannot mint evidence"
    );
    done.send(()).unwrap();
    scope.next_exit().await.unwrap();
    assert!(!receipt.permits(&next, &witness));
    other_done.send(()).unwrap();
    scope.next_exit().await.unwrap();
}
#[test]
fn actual_consuming_registration_panic_keeps_unknown_and_cannot_authorize_candidate_retry() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let mut scope = TaskScope::new(runtime.handle().clone(), NonZeroUsize::new(1).unwrap());
    scope.enable_registration().unwrap();
    let (witness, done) = query(&scope);
    let authority = RootDatagramAuthority::issue();
    let access: Arc<dyn ResourceAccess> = Arc::new(Deny);
    let first = grant(&authority, &access, 0);
    let peer = first.peer();
    let selected = crate::EgressInterfaceControl::default()
        .select_for_peer_with_policy(peer, &DialPolicy::default())
        .unwrap();
    let mut owner = ControlledDatagramOwner::new(NonZeroUsize::new(1).unwrap());
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = owner.acquire(peer, None, &DialPolicy::default(), &selected);
    }));
    assert!(
        panic.is_err(),
        "actual Tokio registration has no entered reactor"
    );
    let acquisition = owner
        .acquire(peer, None, &DialPolicy::default(), &selected)
        .unwrap_err();
    assert!(!owner.confirms_settled_acquisition(&acquisition));
    let failure =
        authority.acquisition_failure(&first.request, &witness, denied(), acquisition, &owner);
    assert!(failure.retry_receipt().is_none());
    let report = owner.take_report();
    assert_eq!(report.confirmed_closed, 0);
    assert_eq!(report.unconfirmed.len(), 1);
    assert!(!report.is_complete());
    done.send(()).unwrap();
    runtime.block_on(scope.next_exit()).unwrap();
}

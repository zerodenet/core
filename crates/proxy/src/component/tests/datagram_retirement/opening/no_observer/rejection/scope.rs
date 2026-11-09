//! Same actual receiver stores the rejected loan before validation and ACK.
use super::*;
use std::{future::Future, num::NonZeroUsize, task::Poll};
use zero_core::component::RoleInstanceId;
use zero_platform_tokio::{
    contract::{
        component::ResourceDeclaration, lifecycle::ResourceLifecycle, scope::TaskScope, Reason,
    },
    controlled_carrier::{loan::DatagramLoan, CarrierOwner},
};

pub(super) async fn reject_fixed_bnd(
    proxy: &Proxy,
    donor: RoleInstanceId,
    parent: zero_platform_tokio::controlled_carrier::CarrierAdoption,
    endpoint: Destination,
    mut binding: UpstreamBinding,
    call: CarrierContext,
    mut loan: DatagramLoan,
) {
    let one = NonZeroUsize::new(1).unwrap();
    let mut scope = TaskScope::suspended_hosted(tokio::runtime::Handle::current(), Some(one));
    scope.enable_registration().unwrap();
    let tasks = scope.registrar();
    let host = Arc::new(CancellationToken::new());
    let mut receiver = CarrierOwner::new(
        proxy.protocols.allocate_resource_identity().unwrap(),
        one,
        &host,
    );
    receiver
        .registration()
        .bind_declared(
            &ResourceDeclaration::ControlledCarrierPool {
                endpoint: endpoint.clone(),
                dial: binding.dial.clone(),
                datagram: true,
                max_sockets: one,
            },
            &tasks,
            &host,
        )
        .unwrap();
    receiver.start(call.deadline.unwrap()).await.unwrap();
    let mut grant = receiver.grants().reserve().unwrap();
    let view = grant.claim_transferred_view().unwrap();
    let adoption = grant.adoption();
    let (mut reader, mut writer) = tokio::io::duplex(1);
    let (pending, suspended) = tokio::sync::oneshot::channel();
    let (_, witness) = adoption
        .spawn_auxiliary(&tasks, async move {
            let mut byte = [0];
            {
                let read = reader.read_exact(&mut byte);
                tokio::pin!(read);
                let mut pending = Some(pending);
                std::future::poll_fn(|cx| {
                    let result = read.as_mut().poll(cx);
                    if result.is_pending() {
                        if let Some(pending) = pending.take() {
                            pending.send(()).unwrap();
                        }
                    }
                    result
                })
                .await
                .unwrap();
            }
            assert_eq!(byte, [9]);
            Ok(())
        })
        .unwrap();
    suspended.await.unwrap();
    assert!(!witness.is_joined());
    binding.prefix = vec![donor];
    let child = loan.owner.original().clone();
    let observed = loan.receiver_observation();
    let error = grant
        .adopt_datagram_loan(&binding, endpoint, &call, loan)
        .err()
        .expect("BND metadata does not authorize Fixed payload borrowing");
    assert_eq!(error.reason, Reason::StalePlan);
    assert_eq!(error.stage, zero_platform_tokio::contract::Stage::Dial);
    assert_eq!(
        error.delivery,
        zero_platform_tokio::contract::DeliveryState::Unknown
    );
    let retained = observed
        .retained()
        .expect("rejected real loan was strongly installed before validation");
    assert!(retained.receiver_adoption().unwrap().same_slot(&adoption));
    assert!(retained.retains(&parent));
    assert!(adoption.closed().is_none() && child.closed().is_none());
    assert_eq!(
        receiver.grants().reserve().err().unwrap().reason,
        Reason::Capacity
    );
    assert!(!adoption.retire(Instant::now()).await.unwrap().is_complete());
    assert!(
        adoption.closed().is_none(),
        "actual auxiliary read still owns its original task duty"
    );
    writer.write_all(&[9]).await.unwrap();
    let receipt = scope.next_receipt().await.unwrap();
    assert!(witness.matches_joined(receipt.joined()) && receipt.result().is_ok());
    assert!(!receipt.joined().registration_released());
    drop(view);
    let closing = adoption.retire(call.deadline.unwrap());
    tokio::pin!(closing);
    std::future::poll_fn(|cx| {
        assert!(
            closing.as_mut().poll(cx).is_pending(),
            "held actual Host ACK still blocks settlement"
        );
        if adoption.closed().is_some() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
    assert!(!receipt.joined().registration_released());
    assert_eq!(
        receiver.grants().reserve().err().unwrap().reason,
        Reason::Capacity
    );
    scope.acknowledge(&receipt).unwrap();
    assert!(witness.try_joined().unwrap().registration_released());
    assert!(closing.await.unwrap().is_complete());
    let closed = adoption.closed().unwrap();
    assert!(closed.matches(&adoption));
    assert!(parent.closed().unwrap().matches(&parent));
    assert!(parent.users_closed().unwrap().matches(&parent));
    assert!(!retained.retains(&parent));
    assert!(receiver
        .shutdown(call.deadline.unwrap())
        .await
        .unwrap()
        .is_complete());
    scope.begin_shutdown();
    assert!(scope.is_drained());
    drop(retained);
}

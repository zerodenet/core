use super::*;
use crate::{
    contract::{scope::TaskScope, ChannelError, Impact, Reason, Stage},
    controlled_datagram::metadata::{MetadataLimits, MetadataOwner},
};
use std::{future::poll_fn, future::Future, time::Duration};
use tokio::{io::AsyncReadExt, sync::oneshot, time::Instant};

fn bound(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}
fn limits() -> MetadataLimits {
    MetadataLimits {
        main_sockets: bound(1),
        probe_sockets: bound(1),
        probe_attempts_per_initialization: bound(1),
        concurrent_send_calls: bound(1),
        write_pollers: bound(1),
    }
}
#[tokio::test]
async fn original_views_and_real_joins_cannot_substitute_held_logical_guards_or_receiver_acceptance(
) {
    let mut scope = TaskScope::suspended_hosted(tokio::runtime::Handle::current(), Some(bound(1)));
    scope.enable_registration().unwrap();
    let registrar = scope.registrar();
    let mut native = MetadataOwner::new(limits()).unwrap();
    let mut grant = native.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let (issuer, claim) = grant
        .control
        .claim_views(registrar.scope_identity())
        .unwrap();
    assert!(claim.matches(&grant.rx, &grant.tx));
    let (graph, mut receiver) = claim
        .bind_users::<u8, ()>(registrar.clone(), bound(1), bound(1))
        .ok()
        .unwrap();
    let completion = graph.completion();
    let logical = graph.users().acquire().unwrap();
    let (mut io, peer) = tokio::io::duplex(1);
    let (pending_tx, pending_rx) = oneshot::channel();
    let id = graph
        .drivers()
        .spawn(
            1,
            crate::contract::scope::TaskClass::Auxiliary,
            |body| async move {
                let read = io.read_u8();
                tokio::pin!(read);
                let mut pending_tx = Some(pending_tx);
                let error = poll_fn(|cx| {
                    let result = read.as_mut().poll(cx);
                    if result.is_pending() {
                        if let Some(pending) = pending_tx.take() {
                            pending.send(()).unwrap();
                        }
                    }
                    result
                })
                .await
                .unwrap_err();
                body.record(());
                Err(
                    ChannelError::new(Stage::Receive, Reason::Io, Impact::Session)
                        .with_source(error),
                )
            },
        )
        .unwrap();
    pending_rx.await.unwrap(); // The actual registered body is suspended in I/O.
    graph.drivers().revoke_registration();
    graph.users().cancellation().cancel();
    drop(grant.rx);
    drop(grant.tx);
    assert!(completion.try_closed().is_none());
    drop(peer); // Actual EOF releases that original registered task.
    let host_receipt = scope.next_receipt().await.unwrap();
    assert_eq!(host_receipt.id(), id);
    assert!(
        completion.try_closed().is_none(),
        "join did not receive outcome"
    );
    let received = receiver
        .receive(id, Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert!(Arc::ptr_eq(
        &host_receipt.result().unwrap_err(),
        &received.joined.result().unwrap_err()
    ));
    assert!(
        completion.try_closed().is_none(),
        "logical guard still held"
    );
    graph.drivers().reclaim_received();
    assert!(completion.try_closed().is_none());
    drop(logical);
    let receipt = completion.try_closed().unwrap();
    assert!(issuer.validates_users(&receipt, &registrar));
    assert_eq!(graph.drivers().task_status().reclaimed, 1);
    assert!(graph.users().acquire().is_err());
    assert_eq!(
        native.occupied_main(),
        1,
        "users receipt does not close native I/O"
    );
    assert!(!scope.is_drained(), "users receipt is not host ACK");
    scope.acknowledge(&host_receipt).unwrap();
    scope.begin_shutdown();
    assert!(scope.is_drained());
    grant.control.close().unwrap();
    assert_eq!(native.take_report().main.confirmed_closed, 1);
}

#[tokio::test]
async fn a_users_receipt_cannot_replay_another_original_or_another_actual_scope() {
    let scope = TaskScope::new(tokio::runtime::Handle::current(), bound(1));
    let registrar = scope.registrar();
    let foreign_scope = TaskScope::new(tokio::runtime::Handle::current(), bound(1));
    let mut native = MetadataOwner::new(limits()).unwrap();
    let mut first = native.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let (issuer, claim) = first
        .control
        .claim_views(registrar.scope_identity())
        .unwrap();
    let claim = claim
        .bind_users::<u8, ()>(foreign_scope.registrar(), bound(1), bound(1))
        .err()
        .unwrap();
    let (graph, _) = claim
        .bind_users::<u8, ()>(registrar.clone(), bound(1), bound(1))
        .ok()
        .unwrap();
    graph.drivers().revoke_registration();
    graph.users().cancellation().cancel();
    drop(first.rx);
    drop(first.tx);
    let receipt = graph.completion().try_closed().unwrap();
    assert!(issuer.validates_users(&receipt, &registrar));
    assert!(!issuer.validates_users(&receipt, &foreign_scope.registrar()));
    first.control.close().unwrap();
    assert_eq!(native.take_report().main.confirmed_closed, 1);
    let mut second = native.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let (other, _) = second
        .control
        .claim_views(registrar.scope_identity())
        .unwrap();
    assert!(!other.validates_users(&receipt, &registrar));
    drop(second.rx);
    drop(second.tx);
    second.control.close().unwrap();
    assert_eq!(native.take_report().main.confirmed_closed, 1);
}

use super::*;
use std::{
    future::{poll_fn, Future},
    task::Poll,
};
#[tokio::test]
async fn actual_accept_returns_pending_charge_and_full_root_budget_does_not_accept_native() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let budget = ControlBudget::new(zero_runtime::configuration::ControlBudgetLimits {
            connections: 1,
            subscriptions: 1,
            queued_event_bytes: 1024,
            pipe_workspace_bytes: 2 * 1024 * 1024,
            messages: Default::default(),
        });
        let charges = Connections::default();
        let (mut scope, mut original) = cold("127.0.0.1:0".parse().unwrap()).unwrap();
        let mut listener = original
            .bind_listener("127.0.0.1:0".parse().unwrap())
            .unwrap();
        let receiver = original.accepted_task_receiver().unwrap();
        let mut pending = Box::pin(
            listener
                .accept_owned_with_admission(|| charges.before(&budget), || charges.no_native()),
        );
        poll_fn(|cx| {
            assert!(pending.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        assert_eq!(
            budget.usage().connections,
            0,
            "known Pending has no native original"
        );
        drop(pending);
        let held = budget.reserve(ControlCharge::Connection).unwrap();
        let client = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let error = listener
            .accept_owned_with_admission(|| charges.before(&budget), || charges.no_native())
            .await
            .err()
            .unwrap();
        assert_eq!(error.reason, Reason::Capacity);
        assert_eq!(
            original.accepted_evidence().unwrap().retained,
            0,
            "full root capacity rejects before native accept"
        );
        assert_eq!(
            original
                .accepted_evidence()
                .unwrap()
                .confirmed_native_closes,
            0
        );
        drop(held);
        let (accepted, _) = listener
            .accept_owned_with_admission(|| charges.before(&budget), || charges.no_native())
            .await
            .unwrap();
        charges.installed(accepted.claim());
        assert_eq!(budget.usage().connections, 1);
        accepted
            .spawn_handler_bound(TaskClass::Auxiliary, |socket, _| async move {
                drop(socket);
                Ok(())
            })
            .unwrap();
        let exit = scope.next_exit().await.unwrap();
        assert!(exit.result.is_ok());
        receiver.observe_joined(exit.id);
        charges.reap(&receiver).await.unwrap();
        assert_eq!(
            budget.usage().connections,
            0,
            "only actual joined users plus matching native close return charge"
        );
        drop(client);
        drop(listener);
        assert!(scope.shutdown(deadline()).await.is_complete());
        assert!(original
            .wait_closed(deadline())
            .await
            .unwrap()
            .is_complete());
    })
    .await
    .unwrap();
}

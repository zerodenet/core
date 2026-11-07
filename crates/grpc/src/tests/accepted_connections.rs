use super::*;
use std::{
    future::{poll_fn, Future},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Weak,
    },
    task::{Context, Poll, Wake, Waker},
};
struct RefundWake {
    actual: Weak<Connections>,
    calls: AtomicUsize,
}
impl Wake for RefundWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let actual = self.actual.upgrade().unwrap();
        assert!(
            actual.acquiring.try_lock().is_ok(),
            "external refund callback must be able to reenter admission"
        );
        assert!(
            actual.entries.try_lock().is_ok(),
            "external refund callback must be able to inspect original custody"
        );
        self.calls.fetch_add(1, Ordering::AcqRel);
    }
}
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
        let charges = Arc::new(Connections::default());
        let observed = Arc::new(RefundWake {
            actual: Arc::downgrade(&charges),
            calls: AtomicUsize::new(0),
        });
        let waker = Waker::from(observed.clone());
        let mut changed = std::pin::pin!(budget.capacity_changed());
        changed.as_mut().enable();
        assert!(changed
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending());
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
            observed.calls.load(Ordering::Acquire),
            1,
            "actual accept Pending refunds outside the owner mutex"
        );
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
        let mut closed = std::pin::pin!(budget.capacity_changed());
        closed.as_mut().enable();
        assert!(closed
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending());
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
            observed.calls.load(Ordering::Acquire),
            2,
            "the matching actual native close refunds outside original custody locks"
        );
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

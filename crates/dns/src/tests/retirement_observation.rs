//! Actual query join observations remain pending while its Host withholds ACK.
use super::*;
use crate::{OwnedDnsQueryBudget, OwnedDnsQueryTasks};
use std::{future::poll_fn, num::NonZeroUsize, task::Poll, time::Duration};
use tokio::io::AsyncRead;
use zero_platform_tokio::contract::{scope::TaskScope, CancellationToken};
#[tokio::test]
async fn real_join_deadline_observations_preserve_actual_host_ack_without_minting_faults() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut scope =
            TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(1));
        scope.enable_registration().unwrap();
        let one = NonZeroUsize::new(1).unwrap();
        let budget = OwnedDnsQueryBudget::new(one);
        let tasks =
            OwnedDnsQueryTasks::new_critical_with_budget(&scope.registrar(), one, budget.clone());
        let (mut reader, _writer) = tokio::io::duplex(1);
        let (entered, pending) = tokio::sync::oneshot::channel();
        tasks
            .spawn(
                async move {
                    let mut entered = Some(entered);
                    let mut byte = [0; 1];
                    poll_fn(|cx| {
                        let mut buffer = tokio::io::ReadBuf::new(&mut byte);
                        match std::pin::Pin::new(&mut reader).poll_read(cx, &mut buffer) {
                            Poll::Pending => {
                                if let Some(entered) = entered.take() {
                                    entered.send(()).unwrap();
                                }
                                Poll::Pending
                            }
                            Poll::Ready(result) => Poll::Ready(result),
                        }
                    })
                    .await
                    .unwrap();
                },
                CancellationToken::new(),
            )
            .unwrap();
        let witness = tasks.observe().into_iter().next().unwrap().task;
        pending.await.unwrap();
        let first = tasks
            .wait_closed(tokio::time::Instant::now())
            .await
            .unwrap_err();
        assert_eq!(first.kind(), io::ErrorKind::TimedOut);
        assert!(first.get_ref().unwrap().is::<DnsRetirementPending>());
        for _ in 0..32 {
            let observed = tasks
                .wait_closed(tokio::time::Instant::now())
                .await
                .unwrap_err();
            assert!(observed.get_ref().unwrap().is::<DnsRetirementPending>());
            assert!(
                witness.try_joined().is_none(),
                "only the original Host consumes the real join"
            );
            assert!(
                tasks.retained_faults().is_none(),
                "deadline observation is not a task fault"
            );
            assert_eq!(tasks.observe().len(), 1);
        }
        let joined = scope.next_receipt().await.unwrap();
        assert!(witness.matches_joined(joined.joined()));
        joined.result().unwrap();
        assert!(!joined.joined().registration_released());
        // Query join and budget ACK are independent: this API confirms join,
        // while the original budget keeps the actual registration charged.
        tasks
            .wait_closed(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(tasks.observe().is_empty());
        let fresh = OwnedDnsQueryTasks::new_critical_with_budget(&scope.registrar(), one, budget);
        assert_eq!(
            fresh
                .spawn(async {}, CancellationToken::new())
                .unwrap_err()
                .kind(),
            io::ErrorKind::WouldBlock
        );
        scope.acknowledge(&joined).unwrap();
        assert!(joined.joined().registration_released());
        fresh.spawn(async {}, CancellationToken::new()).unwrap();
        let next = fresh.observe().into_iter().next().unwrap().task;
        let actual = scope.next_receipt().await.unwrap();
        assert!(next.matches_joined(actual.joined()));
        actual.result().unwrap();
        scope.acknowledge(&actual).unwrap();
        fresh
            .wait_closed(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        scope.begin_shutdown();
        assert!(scope.next_receipt().await.is_none());
    })
    .await
    .unwrap();
}

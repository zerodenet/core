//! The same exact delivered row waits for actual Host ACK, not local acceptance.
use super::*;
#[tokio::test]
async fn actual_delivered_registration_wait_survives_cancel_foreign_receiver_and_real_reclaim() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut scope =
            TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(1));
        scope.enable_registration().unwrap();
        let (owner, mut receiver): (Owner, _) =
            DriverOwner::new(scope.registrar(), NonZeroUsize::new(1).unwrap());
        let source = Arc::new(io::Error::from_raw_os_error(111));
        let id = owner
            .spawn(2, TaskClass::Auxiliary, |body| async move {
                body.record(source);
                Err(ChannelError::new(
                    Stage::Receive,
                    Reason::Io,
                    Impact::Session,
                ))
            })
            .unwrap();
        owner.revoke_registration();
        let host = scope.next_receipt().await.unwrap();
        let received = receiver
            .receive(id, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(host.local_failure_accepted() && !received.joined.registration_released());
        assert!(
            receiver
                .wait_delivery_registration_released(&received, Instant::now())
                .await
                .is_err(),
            "untransferred body cannot be a registration handoff"
        );
        let retained = received.clone(); // Original actual receiver storage first.
        receiver.release_body(&retained).unwrap();
        let (_, foreign): (Owner, _) =
            DriverOwner::new(scope.registrar(), NonZeroUsize::new(1).unwrap());
        assert_eq!(
            foreign
                .wait_delivery_registration_released(&retained, Instant::now())
                .await
                .unwrap_err()
                .reason,
            Reason::StalePlan
        );
        {
            use std::{
                future::{poll_fn, Future},
                task::Poll,
            };
            let mut wait = Box::pin(receiver.wait_delivery_registration_released(
                &retained,
                Instant::now() + Duration::from_secs(1),
            ));
            poll_fn(|cx| {
                assert!(wait.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
        }
        assert!(
            !retained.joined.registration_released(),
            "cancelling a passive wait cannot ACK"
        );
        let deferred = scope.defer_callbacks();
        scope.acknowledge(&host).unwrap();
        assert!(!retained.joined.registration_released());
        drop(deferred);
        let released = receiver
            .wait_delivery_registration_released(&retained, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(released.id(), id);
        assert!(released.registration_released());
        assert!(Arc::ptr_eq(
            &released.result().unwrap_err(),
            &retained.joined.result().unwrap_err()
        ));
        owner.reclaim_received();
        assert_eq!(owner.task_status().retained, 0);
        let again = receiver
            .wait_delivery_registration_released(&retained, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(again.id(), id); // Same private token, never missing-row proof.
        scope.begin_shutdown();
        assert!(scope.is_drained());
    })
    .await
    .unwrap();
}

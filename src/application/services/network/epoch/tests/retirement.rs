//! A real failed task and held native view remain distinct across cleanup retries.
use super::*;
#[tokio::test]
async fn repeated_retirement_retains_each_actual_task_fault_once_while_the_native_view_is_still_held()
 {
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        let epoch = epoch(
            EgressInterfaceControl::default().generation(),
            EpochLimits::default(),
        );
        let (stream, mut peer) = connected(&epoch).await;
        let native = epoch.originals.lock().unwrap()[0].clone();
        let (entered, pending) = tokio::sync::oneshot::channel();
        let witness = epoch
            .spawn(Box::pin(async {
                entered.send(()).unwrap();
                panic!("one original real worker panic");
            }))
            .unwrap();
        pending.await.unwrap(); // The original body enters and panics in this same poll, before cancellation.
        assert!(epoch.retire(end()).await.is_err());
        assert!(witness.is_joined());
        let joined = witness.try_joined().unwrap();
        assert!(joined.result().is_err() && joined.registration_released());
        let retained = joined.result().unwrap_err();
        assert!(!epoch.closed() && native.adoption.closed().is_none());
        for _ in 0..16 {
            assert!(epoch.retire(end()).await.is_err());
            let reports = epoch.task_reports.lock().unwrap();
            assert_eq!(
                reports.len(),
                1,
                "consumed Scope reports are deltas, not historical replay"
            );
            assert_eq!(
                reports
                    .iter()
                    .map(|report| report.failed_tasks.len())
                    .sum::<usize>(),
                1
            );
            assert_eq!(reports[0].failed_tasks[0].id, witness.task_id().unwrap());
            assert!(native.adoption.closed().is_none());
            assert!(!epoch.closed());
        }
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), peer.read(&mut [0; 1]))
                .await
                .is_err()
        );
        drop(stream);
        let fault = epoch.retire(end()).await.unwrap_err();
        assert!(epoch.closed());
        let closed = native.adoption.closed().unwrap();
        assert!(closed.matches(&native.adoption) && closed.had_native_acquisition());
        assert!(
            native
                .adoption
                .users_closed()
                .unwrap()
                .matches(&native.adoption)
        );
        assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
        assert_eq!(epoch.task_reports.lock().unwrap().len(), 1);
        let mut source = Some(&fault as &(dyn std::error::Error + 'static));
        let mut matched = false;
        while let Some(actual) = source {
            if let Some(actual) =
                actual.downcast_ref::<zero_platform_tokio::contract::ChannelError>()
            {
                matched |= std::ptr::eq(actual, retained.as_ref());
            }
            source = actual.source();
        }
        assert!(
            matched,
            "same original joined error Arc is retained after physical retirement"
        );
    })
    .await
    .unwrap();
}

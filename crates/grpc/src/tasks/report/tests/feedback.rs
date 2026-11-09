use super::*;

#[tokio::test]
async fn returned_report_error_can_reenter_its_actual_scope_receiver_without_a_cycle() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let receiver = Arc::new(ReportReceiver::default());
        let receiver_weak = Arc::downgrade(&receiver);
        let directory = tempfile::tempdir().unwrap();
        let os = Arc::new(std::fs::File::open(directory.path().join("missing")).unwrap_err());
        let original = Arc::new(
            ChannelError::new(Stage::Close, Reason::Io, Impact::Component).with_source(os),
        );
        let weak = Arc::downgrade(&original);
        let mut first = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        );
        let source = original.clone();
        first
            .registrar()
            .spawn_tracked(TaskClass::Auxiliary, async move {
                Err(ChannelError::retained(source))
            })
            .unwrap();
        let report = first
            .shutdown(tokio::time::Instant::now() + std::time::Duration::from_secs(2))
            .await;
        let returned = receiver
            .receive("actual source Scope", report)
            .into_result()
            .unwrap_err();
        let mut outer = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        );
        let (id, witness) = outer
            .registrar()
            .spawn_tracked(TaskClass::Critical, async move {
                Err(
                    ChannelError::new(Stage::Close, Reason::Internal, Impact::Component)
                        .with_source(returned),
                )
            })
            .unwrap();
        let report = outer
            .shutdown(tokio::time::Instant::now() + std::time::Duration::from_secs(2))
            .await;
        assert!(report.is_complete());
        assert_eq!(report.failed_tasks[0].id, id);
        let returned = receiver.receive("actual returning Scope", report);
        assert!(returned.complete);
        let returned = returned.into_result().unwrap_err();
        assert_eq!(
            receiver.0.lock().unwrap().tasks.len(),
            2,
            "actual returning TaskId/source is preserved"
        );
        let same = receiver
            .receive("repeated observation", CloseReport::default())
            .into_result()
            .unwrap_err();
        let a = returned
            .get_ref()
            .unwrap()
            .downcast_ref::<ReportFailure>()
            .unwrap();
        let b = same
            .get_ref()
            .unwrap()
            .downcast_ref::<ReportFailure>()
            .unwrap();
        assert!(
            Arc::ptr_eq(a.delivery.as_ref().unwrap(), b.delivery.as_ref().unwrap()),
            "redelivery does not copy another projection"
        );
        assert!(witness.try_joined().unwrap().registration_released());
        drop(original);
        drop(witness);
        drop(first);
        drop(outer);
        drop(same);
        drop(receiver);
        assert!(
            receiver_weak.upgrade().is_none(),
            "returned error owns no mutable receiver or Scope"
        );
        assert!(
            weak.upgrade().is_some(),
            "held returned error still owns the exact actual source"
        );
        drop(returned);
        assert!(
            weak.upgrade().is_none(),
            "returned Scope report creates no source cycle"
        );
    })
    .await
    .unwrap();
}

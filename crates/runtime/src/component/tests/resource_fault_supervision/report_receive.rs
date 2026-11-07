//! Real partial task/resource rows survive a cancelled receiver continuation.
use super::*;
use std::{
    future::{poll_fn, Future},
    task::Poll,
};
use zero_platform_tokio::contract::{
    component::ResourceAccess,
    lifecycle::{ResourceFailure, ResourceFailurePhase, TaskClass, TaskFailure, Unfinished},
    scope::TaskScope,
    CancellationToken, CleanupFailureReceipt,
};
#[tokio::test]
async fn actual_partial_report_sources_survive_next_await_cancellation_and_real_ack_recovery() {
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        let dir = tempfile::tempdir().unwrap();
        let spec = JournalSpec {
            path: dir.path().join("journal"),
            max_bytes: NonZeroUsize::new(64).unwrap(),
            parent_private: false,
        };
        let instance = zero_core::component::ComponentInstanceId {
            component: ComponentId(796),
            generation: ResourceGeneration(3),
        };
        let mut scope =
            TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(1));
        scope.enable_registration().unwrap();
        let tasks = scope.registrar();
        let root = Arc::new(crate::HostOriginalBudgets::default());
        root.carrier
            .install_after_retirement(NonZeroUsize::new(1).unwrap())
            .unwrap();
        let host_cancel = CancellationToken::new();
        let resources = HostResources::new_hosted(
            instance,
            &[ResourceDeclaration::ControlledJournal { spec: spec.clone() }],
            host_cancel.clone(),
            tasks.downgrade(),
            root.clone(),
        )
        .unwrap();
        let end = Instant::now() + std::time::Duration::from_secs(3);
        let mut no_fault = CloseReport::default();
        assert!(resources
            .receive_protocol_report(&tasks.scope_identity(), &mut no_fault)
            .unwrap()
            .is_none());
        assert!(
            !host_cancel.is_cancelled(),
            "a normal report does not seal business admission"
        );
        let access = resources.activate(end).unwrap();
        let grant = resources.journal(0).unwrap();
        std::fs::create_dir(&spec.path).unwrap();
        assert!(grant
            .replace(1, |write| std::io::Write::write_all(write, b"x"))
            .is_err());
        let child = resources.resource_fault().unwrap();
        assert_eq!(child.instance, instance);
        assert_eq!(child.controller, ResourceFailurePhase::DeclaredResources);
        let missing = dir.path().join("task-os-error");
        let source_receipt = CleanupFailureReceipt::new();
        let receipt_for_work = source_receipt.clone();
        let (_, witness) = tasks
            .spawn_tracked(TaskClass::Auxiliary, async move {
                let os = Arc::new(std::fs::File::open(missing).unwrap_err());
                assert!(receipt_for_work.bind_payload(os.clone()));
                Err(
                    ChannelError::new(Stage::Close, Reason::Timeout, Impact::Component)
                        .with_source(os)
                        .with_cleanup_failure_receipt(receipt_for_work),
                )
            })
            .unwrap();
        let receipt = scope.next_receipt().await.unwrap();
        let joined = receipt.joined().clone();
        assert!(witness.matches_joined(&joined));
        let source = joined.result().unwrap_err();
        assert!(!source_receipt.is_acknowledged() && !joined.registration_released());
        let make_report = || CloseReport {
            failed_tasks: vec![TaskFailure {
                id: joined.id(),
                class: joined.class(),
                error: ChannelError::retained(source.clone()),
            }],
            resource_failures: vec![child.clone()],
            unfinished: vec![Unfinished::Recovery(
                "actual Host exit receipt is held".into(),
            )],
            ..Default::default()
        };
        let mut denied = make_report();
        let mut foreign = TaskScope::suspended_hosted(tokio::runtime::Handle::current(), None);
        assert!(resources
            .receive_protocol_report(&foreign.registrar().scope_identity(), &mut denied)
            .is_err());
        assert!(!source_receipt.is_acknowledged());
        assert_eq!(denied.failed_tasks[0].id, joined.id());
        let no_resources = crate::component::instance::resources::NoResources;
        assert!(no_resources
            .receive_protocol_report(&tasks.scope_identity(), &mut denied)
            .is_err());
        assert!(!source_receipt.is_acknowledged());
        drop(denied);
        let (ready, entered) = tokio::sync::oneshot::channel();
        let (resume, waiting) = tokio::sync::oneshot::channel::<()>();
        let received_resources = resources.clone();
        let actual_scope = tasks.scope_identity();
        let partial = make_report();
        let mut operation = Box::pin(async move {
            let mut report = partial;
            received_resources
                .receive_protocol_report(&actual_scope, &mut report)
                .unwrap();
            ready.send(()).unwrap();
            waiting.await.unwrap();
            report
        });
        poll_fn(|cx| {
            assert!(operation.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        entered.await.unwrap();
        assert!(source_receipt.is_acknowledged());
        assert!(
            !joined.registration_released(),
            "diagnostic acceptance is not actual task ACK"
        );
        let receiver = resources.protocol_failure_receiver();
        let canonical = receiver.get().unwrap();
        assert_eq!(receiver.source_count(), 2);
        drop(operation); // Actual partial report and its caller are gone at next await.
        drop(resume);
        assert!(!grant.native_closed());
        let mut retry = make_report();
        retry.resource_failures.push(canonical.clone());
        for _ in 0..64 {
            let same = resources
                .receive_protocol_report(&tasks.scope_identity(), &mut retry)
                .unwrap()
                .unwrap();
            assert!(Arc::ptr_eq(&same, &canonical));
            assert!(Arc::ptr_eq(
                retry.failed_tasks[0].error.retained_source().unwrap(),
                &source
            ));
            assert_eq!(
                receiver.source_count(),
                2,
                "self RF is never inserted into its own Causes"
            );
            assert!(!joined.registration_released());
        }
        let wrapped = Arc::new(ResourceFailure::new(
            instance,
            ResourceFailurePhase::ProtocolLifecycle,
            Arc::new(ChannelError::retained(canonical.source.clone())),
        ));
        let wrapped_weak = Arc::downgrade(&wrapped);
        retry.resource_failures.push(wrapped.clone());
        let same = resources
            .receive_protocol_report(&tasks.scope_identity(), &mut retry)
            .unwrap()
            .unwrap();
        assert!(Arc::ptr_eq(&same, &canonical));
        drop(same);
        assert!(wrapped.ownership_received());
        assert_eq!(
            receiver.source_count(),
            2,
            "wrapped self payload is not another Cause"
        );
        assert!(!joined.registration_released());
        drop(wrapped);
        for self_source in [
            ChannelError::retained(canonical.source.clone()),
            ChannelError::new(Stage::Close, Reason::Io, Impact::Component)
                .with_source(canonical.source.clone()),
        ] {
            let mut malformed = CloseReport {
                failed_tasks: vec![TaskFailure {
                    id: joined.id(),
                    class: joined.class(),
                    error: self_source,
                }],
                ..Default::default()
            };
            assert!(resources
                .receive_protocol_report(&tasks.scope_identity(), &mut malformed)
                .is_err());
            assert_eq!(
                malformed.failed_tasks.len(),
                1,
                "rejection retains the actual input report"
            );
            assert_eq!(receiver.source_count(), 2);
            assert!(!joined.registration_released());
        }
        assert!(canonical.ownership_received());
        let weak = [Arc::downgrade(&source), Arc::downgrade(&child.source)];
        drop(access);
        let mut final_report = resources.shutdown_io(end).await.unwrap();
        assert!(grant.native_closed());
        assert_eq!(
            root.carrier.snapshot().charged,
            1,
            "actual Scope/ACK is still independent"
        );
        receiver.append_task_rows(&mut final_report);
        assert!(final_report
            .failed_tasks
            .iter()
            .any(|row| row.id == joined.id()
                && row.class == joined.class()
                && Arc::ptr_eq(row.error.retained_source().unwrap(), &source)));
        scope.begin_shutdown();
        scope.acknowledge(&receipt).unwrap();
        drop(receipt);
        assert!(scope.next_receipt().await.is_none());
        assert!(joined.registration_released());
        assert_eq!(root.carrier.snapshot().charged, 0);
        assert!(final_report.is_complete());
        let late = resources
            .receive_protocol_report(&tasks.scope_identity(), &mut retry)
            .unwrap()
            .unwrap();
        assert!(Arc::ptr_eq(&late, &canonical));
        drop(late);
        foreign.begin_shutdown();
        assert!(foreign.next_receipt().await.is_none());
        drop(final_report);
        drop(retry);
        assert!(
            wrapped_weak.upgrade().is_none(),
            "receiver did not keep its own RF wrapper"
        );
        drop(receiver);
        drop(resources);
        drop(grant);
        drop(root);
        drop(source);
        drop(child);
        drop(witness);
        drop(joined);
        drop(tasks);
        drop(scope);
        assert!(weak.iter().all(|source| source.upgrade().is_some()));
        drop(canonical);
        assert!(
            weak.iter().all(|source| source.upgrade().is_none()),
            "pure receiver/report rows form no Scope or RF cycle"
        );
    })
    .await
    .unwrap();
}

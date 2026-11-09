//! Real same-Host multi-journal IO failures and independent Hosted ACK.
use super::*;
use std::num::NonZeroUsize;
use zero_core::component::{ComponentInstanceId, ResourceGeneration};
use zero_platform_tokio::contract::{
    component::ResourceAccess, lifecycle::TaskClass, scope::TaskScope,
};
use zero_platform_tokio::journal::JournalSpec;
#[tokio::test]
async fn two_actual_journal_faults_have_one_canonical_receiver_and_held_ack_keeps_credit() {
    let directory = tempfile::tempdir().unwrap();
    let actual = ComponentInstanceId {
        component: ComponentId(791),
        generation: ResourceGeneration(2),
    };
    let declarations: Vec<_> = ["first", "second"]
        .into_iter()
        .map(|name| ResourceDeclaration::ControlledJournal {
            spec: JournalSpec {
                path: directory.path().join(name),
                max_bytes: NonZeroUsize::new(64).unwrap(),
                parent_private: false,
            },
        })
        .collect();
    let originals = Arc::new(crate::HostOriginalBudgets::default());
    originals
        .carrier
        .install_after_retirement(NonZeroUsize::new(2).unwrap())
        .unwrap();
    let mut scope =
        TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(2));
    scope.enable_registration().unwrap();
    let tasks = scope.registrar();
    let resources = HostResources::new_hosted(
        actual,
        &declarations,
        CancellationToken::new(),
        tasks.downgrade(),
        originals.clone(),
    )
    .unwrap();
    let end = Instant::now() + std::time::Duration::from_secs(3);
    let capsule = resources.declared_resource_sources().unwrap();
    assert_eq!(capsule.instance(), actual);
    assert!(
        resources.resource_fault().is_none(),
        "getter did not create a fault or acquire IO"
    );
    let access = resources.activate(end).unwrap();
    let grants = [resources.journal(0).unwrap(), resources.journal(1).unwrap()];
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let mut witnesses = Vec::new();
    let mut completed = Vec::new();
    for grant in grants.iter().cloned() {
        let barrier = barrier.clone();
        let (done, finished) = tokio::sync::oneshot::channel();
        completed.push(finished);
        let (_, witness) = tasks
            .spawn_tracked(TaskClass::Critical, async move {
                let failed = tokio::task::spawn_blocking(move || {
                    grant.replace(1, |write| {
                        // Both real calls hold their acquired temporary original before
                        // either OS failure can close this actual Host's admission.
                        barrier.wait();
                        std::fs::create_dir(grant.path())?;
                        std::io::Write::write_all(write, b"x")
                    })
                })
                .await
                .unwrap();
                assert!(
                    failed.is_err(),
                    "rename of original temporary file onto directory must fail in OS"
                );
                assert!(done.send(()).is_ok());
                Ok(())
            })
            .unwrap();
        witnesses.push(witness);
    }
    drop(access);
    // Actual calls finish; wrapper outcomes remain un-ACKed in the same Scope.
    for finished in completed {
        tokio::time::timeout_at(end, finished)
            .await
            .unwrap()
            .unwrap();
    }
    scope.begin_shutdown();
    let one = tokio::time::timeout_at(end, scope.next_receipt())
        .await
        .unwrap()
        .unwrap();
    let two = tokio::time::timeout_at(end, scope.next_receipt())
        .await
        .unwrap()
        .unwrap();
    assert!(witnesses
        .iter()
        .any(|witness| witness.matches_joined(one.joined())));
    assert!(witnesses
        .iter()
        .any(|witness| witness.matches_joined(two.joined())));
    let faults = originals.carrier.failures_with_instances();
    let typed = originals.carrier.resource_failures();
    assert_eq!(typed.len(), 2);
    assert_eq!(faults.len(), 2);
    assert!(!faults[0].source.ptr_eq(&faults[1].source));
    assert!(faults.iter().all(|fault| fault.instance == Some(actual)));
    assert!(typed.iter().all(|fault| !fault.ownership_received()));
    assert_eq!(originals.carrier.snapshot().charged, 2);
    let canonical = resources.resource_fault().unwrap();
    assert!(typed.iter().all(|fault| fault.ownership_received()));
    assert!(!canonical.reported());
    let causes = resources
        .declared_failures
        .causes
        .sources
        .lock()
        .unwrap()
        .len();
    for _ in 0..32 {
        resources.retain_resource_error(Arc::new(ChannelError::retained(typed[0].source.clone())));
        resources.retain_resource_error(Arc::new(ChannelError::retained(typed[1].source.clone())));
        resources.retain_resource_error(Arc::new(ChannelError::retained(canonical.source.clone())));
    }
    assert_eq!(
        resources
            .declared_failures
            .causes
            .sources
            .lock()
            .unwrap()
            .len(),
        causes,
        "same original fault retries and canonical forwarding cannot grow the receiver or cycle"
    );
    let report = resources.shutdown_io(end).await.unwrap();
    assert!(report.is_complete());
    assert!(grants.iter().all(|grant| grant.native_closed()));
    assert_eq!(
        originals.carrier.snapshot().charged,
        2,
        "positive native and local fault handoff do not ACK actual callers"
    );
    let mut final_report = report;
    resources.retain_resource_report(&mut final_report);
    assert_eq!(final_report.resource_failures.len(), 1);
    assert!(Arc::ptr_eq(&canonical, &final_report.resource_failures[0]));
    assert!(Arc::ptr_eq(
        &canonical,
        &resources.resource_fault().unwrap()
    ));
    assert!(scope.acknowledge(&one).is_ok());
    assert_eq!(originals.carrier.snapshot().charged, 2);
    assert!(scope.acknowledge(&two).is_ok());
    assert!(scope.next_receipt().await.is_none());
    assert_eq!(originals.carrier.snapshot().charged, 0);
    let weak: Vec<_> = faults
        .iter()
        .map(|fault| fault.source.downgrade())
        .collect();
    drop(faults);
    drop(typed);
    drop(resources);
    drop(grants);
    assert!(
        weak.iter().all(|source| source.upgrade().is_some()),
        "actual receiver strongly keeps both independent OS faults after producer disposal"
    );
    drop(final_report);
    assert!(weak.iter().all(|source| source.upgrade().is_some()));
    drop(canonical);
    assert!(
        weak.iter().all(|source| source.upgrade().is_some()),
        "pre-IO capsule holds both later exact OS sources after the writer retires"
    );
    drop(capsule);
    assert!(
        weak.iter().all(|source| source.upgrade().is_none()),
        "no receipt/receiver/producer Arc cycle"
    );
}

#[test]
fn pure_declared_capsule_debug_never_formats_foreign_sources_under_the_receiver_lock() {
    struct Reenter(std::sync::Weak<DeclaredReceiver>);
    impl std::fmt::Debug for Reenter {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let actual = self.0.upgrade().unwrap();
            assert!(
                actual.causes.sources.try_lock().is_ok(),
                "foreign Debug must not run under the actual receiver mutex"
            );
            f.write_str("foreign source")
        }
    }
    impl std::fmt::Display for Reenter {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("foreign source")
        }
    }
    impl std::error::Error for Reenter {}
    let receiver = Arc::new(DeclaredReceiver::default());
    let actual = ComponentInstanceId {
        component: ComponentId(792),
        generation: ResourceGeneration(2),
    };
    receiver.original(
        actual,
        Arc::new(
            ChannelError::new(
                zero_platform_tokio::contract::Stage::Close,
                Reason::Io,
                zero_platform_tokio::contract::Impact::Component,
            )
            .with_source(Reenter(Arc::downgrade(&receiver))),
        ),
    );
    let capsule = receiver.sources(actual);
    assert!(format!("{:?}", receiver.causes).contains("retained"));
    assert!(format!("{capsule:?}").contains("instance"));
}

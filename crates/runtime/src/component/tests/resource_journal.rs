//! Actual hosted declaration authority, native evidence and Scope are separate.
use super::*;
use std::num::NonZeroUsize;
use zero_core::component::{ComponentInstanceId, ResourceGeneration};
use zero_platform_tokio::{contract::scope::TaskScope, journal::JournalSpec};
#[tokio::test]
async fn exact_journal_declaration_stays_in_same_host_root_until_native_and_scope_retirement() {
    let directory = tempfile::tempdir().unwrap();
    let spec = JournalSpec {
        path: directory.path().join("journal"),
        max_bytes: NonZeroUsize::new(64).unwrap(),
        parent_private: false,
    };
    let actual = ComponentInstanceId {
        component: ComponentId(789),
        generation: ResourceGeneration(2),
    };
    let mut scope = TaskScope::suspended_hosted(tokio::runtime::Handle::current(), None);
    scope.enable_registration().unwrap();
    let tasks = scope.registrar();
    let originals = Arc::new(crate::HostOriginalBudgets::default());
    let resources = HostResources::new_hosted(
        actual,
        &[ResourceDeclaration::ControlledJournal { spec: spec.clone() }],
        CancellationToken::new(),
        tasks.downgrade(),
        originals.clone(),
    )
    .unwrap();
    assert!(!spec.path.with_extension("lock").exists());
    assert_eq!(originals.carrier.snapshot().charged, 0);
    assert!(
        resources.journal(0).is_err(),
        "cold resource metadata grants no IO"
    );
    let deadline = Instant::now() + std::time::Duration::from_secs(3);
    let access = resources.activate(deadline).unwrap();
    assert!(resources.journal(1).is_err(), "no undeclared ordinal");
    let grant = resources.journal(0).unwrap();
    assert_eq!(grant.host_scope().unwrap().0, actual);
    assert!(grant.host_scope().unwrap().1.matches(&tasks));
    grant
        .replace(5, |write| std::io::Write::write_all(write, b"line\n"))
        .unwrap();
    assert_eq!(originals.carrier.snapshot().charged, 1);
    assert!(!resources.io_evidence().physical_confirmed());
    drop(access);
    let report = resources.shutdown_io(deadline).await.unwrap();
    assert!(report.is_complete());
    assert!(grant.native_closed());
    assert!(resources.io_evidence().physical_confirmed());
    assert_eq!(
        originals.carrier.snapshot().charged,
        1,
        "native close does not close the actual Scope"
    );
    scope.begin_shutdown();
    assert!(scope.next_receipt().await.is_none());
    assert_eq!(originals.carrier.snapshot().charged, 0);
}

//! Original resource fault supervision and actual fresh Slot recovery.
use super::*;
use std::{num::NonZeroUsize, path::PathBuf, sync::Mutex as StdMutex};
use zero_platform_tokio::{
    contract::{
        component::{
            ComponentFactory, ComponentRoles, ComponentSpec, ResourceDeclaration, ResourceOwner,
        },
        lifecycle::{erase_lifecycle, BuildError, ResourceLifecycle},
    },
    journal::{JournalGrant, JournalSpec},
};
struct Factory(PathBuf, Arc<StdMutex<Option<JournalGrant>>>);
struct Lifecycle {
    resources: Arc<dyn zero_platform_tokio::contract::component::ResourceAccess>,
    grant: Arc<StdMutex<Option<JournalGrant>>>,
}
impl ComponentFactory for Factory {
    type Options = ();
    fn describe(&self, _: &()) -> ComponentSpec {
        ComponentSpec {
            owner: ResourceOwner::Component,
            roles: vec![],
            dependencies: vec![],
            static_identity: Arc::from([]),
            task_limit: None,
            resources: vec![ResourceDeclaration::ControlledJournal {
                spec: JournalSpec {
                    path: self.0.clone(),
                    max_bytes: NonZeroUsize::new(64).unwrap(),
                },
            }],
        }
    }
    fn build(&self, _: Arc<()>, context: BuildContext) -> Result<ComponentInstance, BuildError> {
        Ok(ComponentInstance {
            roles: ComponentRoles::default(),
            lifecycle: erase_lifecycle(Lifecycle {
                resources: context.resources,
                grant: self.1.clone(),
            }),
        })
    }
}
impl ResourceLifecycle for Lifecycle {
    async fn start(&mut self, _: Instant) -> Result<(), BuildError> {
        *self.grant.lock().unwrap() = Some(self.resources.journal(0)?);
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {}
    async fn shutdown(&mut self, _: Instant) -> Result<CloseReport, BuildError> {
        Ok(CloseReport::default())
    }
}
fn slot(path: PathBuf, generation: u64) -> (ComponentSlot, Arc<StdMutex<Option<JournalGrant>>>) {
    let grant = Arc::new(StdMutex::new(None));
    (
        ComponentSlot::from_recipe(
            ComponentId(792),
            ResourceGeneration(generation),
            ComponentRecipe::from_validated(Arc::new(Factory(path, grant.clone())), Arc::new(())),
            Default::default(),
            "journal-original",
        ),
        grant,
    )
}
#[tokio::test]
async fn original_fault_blocks_reuse_without_fake_task_and_actual_close_allows_fresh_native() {
    use crate::component::catalog::supervision::testing::TestContext;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("journal");
    let (old, original) = slot(path.clone(), 2);
    let host = TestContext::new([&old]);
    let root = old.bound_original_budgets().unwrap();
    root.carrier
        .install_after_retirement(NonZeroUsize::new(1).unwrap())
        .unwrap();
    let end = Instant::now() + std::time::Duration::from_secs(3);
    host.drive(old.start(end)).await.unwrap();
    let grant = original.lock().unwrap().as_ref().unwrap().clone();
    std::fs::create_dir(&path).unwrap();
    assert!(grant
        .append(1, |writer| std::io::Write::write_all(writer, b"x"))
        .is_err());
    let fault = old.observe_resource_fault().unwrap();
    assert!(
        old.task_fault().is_none(),
        "resource IO is not a fabricated task exit"
    );
    assert!(!old.can_reuse());
    assert!(!old.has_started());
    let rejected = host.drive(old.start(end)).await.unwrap_err();
    let same = rejected
        .retained_source()
        .expect("exact canonical resource fault Arc");
    assert!(Arc::ptr_eq(same, &fault.source));
    assert_eq!(root.carrier.snapshot().charged, 1);
    let report = host.drive(old.shutdown(end)).await.unwrap();
    assert!(report.is_complete());
    assert!(grant.native_closed());
    assert_eq!(report.resource_failures.len(), 1);
    assert!(Arc::ptr_eq(&fault, &report.resource_failures[0]));
    assert_eq!(root.carrier.snapshot().charged, 0);
    std::fs::remove_dir(&path).unwrap();
    let (fresh, current) = slot(path.clone(), 3);
    fresh.bind_original_budgets(root.clone());
    let fresh_host = TestContext::new([&fresh]);
    fresh_host.drive(fresh.start(end)).await.unwrap();
    let current_grant = current.lock().unwrap().as_ref().unwrap().clone();
    assert!(!grant.same_original(&current_grant));
    assert!(!current_grant.native_closed());
    current_grant
        .replace(4, |writer| std::io::Write::write_all(writer, b"new\n"))
        .unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), b"new\n");
    assert_eq!(root.carrier.snapshot().charged, 1);
    assert!(fresh_host
        .drive(fresh.shutdown(end))
        .await
        .unwrap()
        .is_complete());
    assert!(current_grant.native_closed());
    assert_eq!(root.carrier.snapshot().charged, 0);
    assert!(Arc::ptr_eq(&fault, &report.resource_failures[0]));
}

#![cfg(unix)]
use crate::component::{catalog::ComponentCatalog, instance::HostResources};
use std::{error::Error, num::NonZeroUsize, path::PathBuf};
use tokio::time::{Duration, Instant};
use zero_platform_tokio::contract::{
    component::{ProcessRequest, ResourceAccess, ResourceDeclaration},
    CancellationToken, Reason,
};

fn sleeping() -> ProcessRequest {
    let mut request = ProcessRequest::new("python3");
    request.arguments = vec!["-c".into(), "import time; time.sleep(60)".into()];
    request
}
#[tokio::test]
async fn actual_native_grants_require_start_phase_exact_program_and_declared_lifetime_budget() {
    let component = ComponentCatalog::default().allocate_id().unwrap();
    let cancel = CancellationToken::new();
    let grants = HostResources::new(
        component,
        &[ResourceDeclaration::Process {
            program: "python3".into(),
            max_starts: NonZeroUsize::new(1).unwrap(),
        }],
        cancel.clone(),
    )
    .unwrap();
    let error = grants.spawn_process(sleeping()).unwrap_err();
    assert_eq!(error.reason, Reason::InvalidConfig);
    assert_eq!(error.component, Some(component));
    let access = grants
        .activate(Instant::now() + Duration::from_secs(5))
        .unwrap();
    assert_eq!(
        grants
            .spawn_process(ProcessRequest::new("ungranted-program"))
            .unwrap_err()
            .reason,
        Reason::InvalidConfig
    );
    let mut child = grants.spawn_process(sleeping()).unwrap();
    assert!(child.id().is_some());
    assert_eq!(
        grants.spawn_process(sleeping()).unwrap_err().reason,
        Reason::Capacity
    );
    drop(access);
    assert_eq!(
        grants.spawn_process(sleeping()).unwrap_err().reason,
        Reason::InvalidConfig
    );
    child.start_kill().unwrap();
    tokio::time::timeout(Duration::from_secs(3), child.wait())
        .await
        .unwrap()
        .unwrap();
    // Reaping does not silently replenish a generation's native start budget.
    let access = grants
        .activate(Instant::now() + Duration::from_secs(5))
        .unwrap();
    assert_eq!(
        grants.spawn_process(sleeping()).unwrap_err().reason,
        Reason::Capacity
    );
    cancel.cancel();
    assert_eq!(
        grants.spawn_process(sleeping()).unwrap_err().reason,
        Reason::Revoked
    );
    drop(access);
}

#[tokio::test]
async fn failed_native_spawn_keeps_its_actual_os_source_and_does_not_consume_a_child_start() {
    let component = ComponentCatalog::default().allocate_id().unwrap();
    let program = PathBuf::from("/zero-test-absent-native-process");
    let grants = HostResources::new(
        component,
        &[ResourceDeclaration::Process {
            program: program.clone(),
            max_starts: NonZeroUsize::new(1).unwrap(),
        }],
        CancellationToken::new(),
    )
    .unwrap();
    let access = grants
        .activate(Instant::now() + Duration::from_secs(5))
        .unwrap();
    for _ in 0..2 {
        let error = grants
            .spawn_process(ProcessRequest::new(program.clone()))
            .unwrap_err();
        assert_eq!(error.reason, Reason::Io);
        assert_eq!(error.component, Some(component));
        let source = error
            .source()
            .unwrap()
            .downcast_ref::<std::io::Error>()
            .unwrap();
        assert_eq!(source.kind(), std::io::ErrorKind::NotFound);
    }
    drop(access);
    assert!(grants.activate(Instant::now()).is_err());
}

#[test]
fn native_launch_debug_never_exposes_arguments_or_environment_values() {
    let mut request = sleeping();
    request.arguments.push("secret-argument".into());
    request
        .environment
        .insert("PRIVATE".into(), Some("secret-environment".into()));
    let text = format!("{request:?}");
    assert!(!text.contains("secret-argument"));
    assert!(!text.contains("secret-environment"));
    assert!(!text.contains("PRIVATE"));
    assert!(text.contains("argument_count"));
}

#[tokio::test]
async fn the_actual_slot_denies_native_starts_during_build_and_after_start_returns() {
    use crate::component::{
        instance::ComponentSlot, ComponentRecipe, DirectFactory, DirectOptions,
    };
    use std::sync::{Arc, Mutex};
    use zero_core::component::ResourceGeneration;
    use zero_platform_tokio::contract::component::{
        BuildContext, ComponentFactory, ComponentInstance, ComponentSpec,
    };
    struct Factory(Arc<Mutex<Option<Arc<dyn ResourceAccess>>>>);
    impl ComponentFactory for Factory {
        type Options = DirectOptions;
        fn describe(&self, options: &DirectOptions) -> ComponentSpec {
            let mut spec = DirectFactory.describe(options);
            spec.resources.push(ResourceDeclaration::Process {
                program: "python3".into(),
                max_starts: NonZeroUsize::new(1).unwrap(),
            });
            spec
        }
        fn build(
            &self,
            options: Arc<DirectOptions>,
            context: BuildContext,
        ) -> Result<ComponentInstance, zero_platform_tokio::contract::ChannelError> {
            let error = context.resources.spawn_process(sleeping()).unwrap_err();
            assert_eq!(error.reason, Reason::InvalidConfig);
            assert_eq!(error.component, Some(context.component));
            *self.0.lock().unwrap() = Some(context.resources.clone());
            DirectFactory.build(options, context)
        }
    }
    let retained = Arc::new(Mutex::new(None));
    let recipe = ComponentRecipe::from_validated(
        Arc::new(Factory(retained.clone())),
        Arc::new(DirectOptions::validate(Default::default()).unwrap()),
    );
    let id = ComponentCatalog::default().allocate_id().unwrap();
    let slot = ComponentSlot::from_recipe(
        id,
        ResourceGeneration(1),
        recipe,
        Default::default(),
        "test_native_grants",
    );
    slot.start(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    let access = retained.lock().unwrap().clone().unwrap();
    assert_eq!(
        access.spawn_process(sleeping()).unwrap_err().reason,
        Reason::InvalidConfig
    );
    assert!(
        !slot
            .shutdown(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap()
            .is_complete(),
        "uncovered Process declarations cannot manufacture native closure"
    );
    assert_eq!(
        access.spawn_process(sleeping()).unwrap_err().reason,
        Reason::Revoked
    );
}

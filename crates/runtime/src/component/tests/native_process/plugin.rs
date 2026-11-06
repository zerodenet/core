#![cfg(unix)]
use crate::component::catalog::supervision::testing::TestContext;
use crate::component::{ComponentRecipe, catalog::ComponentCatalog, instance::ComponentSlot};
use shadowsocks::{
    component::PluginFactory,
    transport::{ShadowsocksTransportState, plugin::PluginTermination},
    validation::{PluginConfig, PluginMode},
};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::time::{Duration, Instant};
use zero_core::component::ResourceGeneration;

struct Marker(PathBuf);
impl Marker {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "zero-native-plugin-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        Self(path)
    }
    fn config(&self, tail: &str) -> PluginConfig {
        PluginConfig {
            command: "python3".into(),
            args: vec![
                "-c".into(),
                format!(
                    "import os,socket,time; s=socket.socket(); s.bind((os.environ['SS_LOCAL_HOST'],int(os.environ['SS_LOCAL_PORT']))); s.listen(); open({:?},'a').write(str(os.getpid())+'\\n'); {tail}",
                    self.0.to_str().unwrap()
                ),
            ],
            options: None,
            mode: PluginMode::TcpOnly,
        }
    }
    async fn pid(&self) -> u32 {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let Some(pid) = std::fs::read_to_string(&self.0)
                    .unwrap()
                    .lines()
                    .next()
                    .and_then(|p| p.parse().ok())
                {
                    return pid;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap()
    }
    fn starts(&self) -> usize {
        std::fs::read_to_string(&self.0).unwrap().lines().count()
    }
}
impl Drop for Marker {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
        let _ = std::fs::remove_file(self.0.with_extension("exit"));
    }
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}
fn slot(state: &ShadowsocksTransportState, config: PluginConfig) -> Arc<ComponentSlot> {
    let ids = ComponentCatalog::default();
    Arc::new(ComponentSlot::from_recipe(
        ids.allocate_id().unwrap(),
        ResourceGeneration(1),
        ComponentRecipe::from_validated(
            Arc::new(PluginFactory),
            Arc::new(state.plugin_component_options("127.0.0.1", 9999, config)),
        ),
        Default::default(),
        "native_plugin",
    ))
}

#[tokio::test]
async fn actual_host_slot_grants_one_real_plugin_child_and_joins_its_actual_monitor_before_reporting_exit()
 {
    let marker = Marker::new();
    let config = marker.config("time.sleep(60)");
    let state = ShadowsocksTransportState::default();
    let resource = state.plugin_resource("127.0.0.1", 9999, config.clone());
    let slot = slot(&state, config);
    let host = TestContext::new([slot.as_ref()]);
    assert!(slot.recipe.spec().roles.is_empty());
    host.drive(slot.start(deadline())).await.unwrap();
    let pid = marker.pid().await;
    assert_ne!(pid, 0);
    host.drive(slot.start(deadline())).await.unwrap();
    assert_eq!(marker.starts(), 1);
    let report = host.drive(slot.shutdown(deadline())).await.unwrap();
    // The actual owned process tree, native originals and same-Scope monitor
    // must all be complete; a protocol child status alone cannot qualify this.
    assert!(report.is_complete(), "{report:?}");
    assert_eq!(report.closed_io.len(), 1);
    assert_eq!(
        slot.bound_original_budgets()
            .unwrap()
            .process
            .snapshot()
            .charged_jobs,
        0
    );
    assert_eq!(report.exited_tasks.len(), 1);
    assert_eq!(report.released_resources.len(), 1);
    assert!(matches!(
        resource.shutdown(deadline()).await.unwrap(),
        PluginTermination::Exited(_)
    ));
}

#[tokio::test]
async fn rejected_native_adoption_and_its_discarded_owner_never_revoke_the_legacy_child() {
    let marker = Marker::new();
    let config = marker.config("time.sleep(60)");
    let state = ShadowsocksTransportState::default();
    let resource = state.plugin_resource("127.0.0.1", 9999, config.clone());
    resource.start().await.unwrap();
    let pid = marker.pid().await;
    let candidate = slot(&state, config.clone());
    let error = candidate.start(deadline()).await.unwrap_err();
    assert_eq!(
        error.reason,
        zero_platform_tokio::contract::Reason::StalePlan
    );
    let report = candidate.shutdown(deadline()).await.unwrap();
    assert!(report.released_resources.is_empty() && report.exited_tasks.is_empty());
    drop(candidate);
    resource.start().await.unwrap();
    assert_eq!(marker.pid().await, pid);
    assert_eq!(marker.starts(), 1);
    assert!(matches!(
        resource.shutdown(deadline()).await.unwrap(),
        PluginTermination::Exited(_)
    ));
}

#[tokio::test]
async fn actual_registered_plugin_monitor_reports_the_actual_child_exit_with_its_real_component_identity()
 {
    use crate::component::instance::HostResources;
    use zero_core::component::ComponentRoleIds;
    use zero_platform_tokio::contract::{
        CancellationToken,
        component::{BuildContext, ComponentFactory},
        lifecycle::TaskClass,
        scope::TaskScope,
    };
    let marker = Marker::new();
    let exit_gate = marker.0.with_extension("exit");
    let config = marker.config(&format!(
        "\nwhile not os.path.exists({:?}): time.sleep(.01)\nos._exit(23)",
        exit_gate.to_str().unwrap()
    ));
    let state = ShadowsocksTransportState::default();
    let resource = state.plugin_resource("127.0.0.1", 9999, config.clone());
    let options = Arc::new(state.plugin_component_options("127.0.0.1", 9999, config));
    let component = ComponentCatalog::default().allocate_id().unwrap();
    let cancel = CancellationToken::new();
    let mut scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        std::num::NonZeroUsize::new(1).unwrap(),
    );
    let grants = HostResources::new_for_instance(
        zero_core::component::ComponentInstanceId {
            component,
            generation: ResourceGeneration(1),
        },
        &PluginFactory.describe(&options).resources,
        cancel.clone(),
        scope.registrar().scope_identity(),
    )
    .unwrap();
    let mut instance = PluginFactory
        .build(
            options,
            BuildContext {
                component,
                generation: ResourceGeneration(1),
                dependencies: Arc::from([]),
                roles: ComponentRoleIds::default(),
                resources: grants.clone(),
                tasks: scope.registrar(),
                cancel,
                background_carrier: None,
            },
        )
        .unwrap();
    let access = grants.activate(deadline()).unwrap();
    instance.lifecycle.start(deadline()).await.unwrap();
    drop(access);
    marker.pid().await;
    // The actual child exits with its own status. Revoking its owner would
    // produce a different wait outcome rather than this original process fault.
    std::fs::write(exit_gate, b"exit with original status").unwrap();
    let exit = tokio::time::timeout_at(deadline(), scope.next_exit())
        .await
        .unwrap()
        .unwrap();
    let PluginTermination::Exited(status) = resource.shutdown(deadline()).await.unwrap() else {
        panic!("actual process original did not retire");
    };
    assert_eq!(status.code(), Some(23));
    assert_eq!(exit.class, TaskClass::Critical);
    let error = exit.result.unwrap_err();
    assert_eq!(error.component, Some(component));
    assert_eq!(error.reason, zero_platform_tokio::contract::Reason::Io);
    assert_eq!(error.stage, zero_platform_tokio::contract::Stage::Receive);
    let mut source = std::error::Error::source(&error);
    let mut original_status = None;
    for _ in 0..32 {
        let Some(original) = source else {
            break;
        };
        if let Some(original) = original.downcast_ref::<shadowsocks::component::PluginRootExit>() {
            original_status = Some(original.status());
            break;
        }
        source = original.source();
    }
    assert_eq!(
        original_status,
        Some(status),
        "actual typed exit missing: {error:?}"
    );
    assert_eq!(marker.starts(), 1);
    assert!(
        instance
            .lifecycle
            .shutdown(deadline())
            .await
            .unwrap()
            .is_complete()
    );
    assert!(scope.shutdown(deadline()).await.is_complete());
}

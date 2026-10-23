#![cfg(all(feature = "shadowsocks", unix))]
use crate::component::catalog::supervision::testing::TestContext;
use crate::component::{catalog::ComponentCatalog, instance::ComponentSlot, ComponentRecipe};
use shadowsocks::{
    component::PluginFactory,
    transport::{plugin::PluginTermination, ShadowsocksTransportState},
    validation::{PluginConfig, PluginMode},
};
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
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
            args: vec!["-c".into(), format!("import os,socket,time; s=socket.socket(); s.bind((os.environ['SS_LOCAL_HOST'],int(os.environ['SS_LOCAL_PORT']))); s.listen(); open({:?},'a').write(str(os.getpid())+'\\n'); {tail}", self.0.to_str().unwrap())],
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
async fn actual_host_slot_grants_one_real_plugin_child_and_joins_its_actual_monitor_before_reporting_exit(
) {
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
    // The protocol reaped its actual Child and the host joined its monitor.
    // Bare Process grants still have no native provider witness; keep that
    // declaration explicitly unaccounted rather than infer whole-owner close.
    assert!(!report.is_complete());
    assert!(report.unfinished.iter().any(|item| matches!(item,
        zero_platform_tokio::contract::lifecycle::Unfinished::Resource(message)
            if message.contains("declaration coverage remains unconfirmed")
    )));
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
async fn actual_registered_plugin_monitor_reports_the_actual_child_exit_with_its_real_component_identity(
) {
    use crate::component::instance::HostResources;
    use zero_core::component::ComponentRoleIds;
    use zero_platform_tokio::contract::{
        component::{BuildContext, ComponentFactory},
        lifecycle::TaskClass,
        scope::TaskScope,
        CancellationToken,
    };
    let marker = Marker::new();
    let config = marker.config("time.sleep(60)");
    let state = ShadowsocksTransportState::default();
    let resource = state.plugin_resource("127.0.0.1", 9999, config.clone());
    let options = Arc::new(state.plugin_component_options("127.0.0.1", 9999, config));
    let component = ComponentCatalog::default().allocate_id().unwrap();
    let cancel = CancellationToken::new();
    let grants = HostResources::new(
        component,
        &PluginFactory.describe(&options).resources,
        cancel.clone(),
    )
    .unwrap();
    let mut scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        std::num::NonZeroUsize::new(1).unwrap(),
    );
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
    // External termination is a real process fault from the component's view.
    assert!(matches!(
        resource.shutdown(deadline()).await.unwrap(),
        PluginTermination::Exited(_)
    ));
    let exit = tokio::time::timeout_at(deadline(), scope.next_exit())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(exit.class, TaskClass::Critical);
    let error = exit.result.unwrap_err();
    assert_eq!(error.component, Some(component));
    assert_eq!(error.reason, zero_platform_tokio::contract::Reason::Io);
    assert!(std::error::Error::source(&error)
        .unwrap()
        .to_string()
        .contains("native plugin child exited:"));
    assert_eq!(marker.starts(), 1);
    assert!(instance
        .lifecycle
        .shutdown(deadline())
        .await
        .unwrap()
        .is_complete());
    assert!(scope.shutdown(deadline()).await.is_complete());
}

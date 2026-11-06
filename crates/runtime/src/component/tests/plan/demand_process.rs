//! Real plan activation must keep an unselected original process cold.
use super::*;
use crate::component::{ComponentRecipe, DirectFactory, DirectOptions};
use shadowsocks::{
    component::PluginFactory,
    transport::ShadowsocksTransportState,
    validation::{PluginConfig, PluginMode},
};
use zero_core::component::{NetworkGeneration, ResourceGeneration};
#[tokio::test]
async fn actual_plan_ready_scans_unselected_process_cold_then_supervises_its_same_on_demand_actor()
{
    let directory = tempfile::tempdir().unwrap();
    let marker = directory.path().join("actual-child");
    let config = PluginConfig {
        command: "python3".into(),
        args: vec![
            "-c".into(),
            format!(
                "import os,socket,time; s=socket.socket(); s.bind((os.environ['SS_LOCAL_HOST'],int(os.environ['SS_LOCAL_PORT']))); s.listen(); open({:?},'w').write(str(os.getpid())); time.sleep(60)",
                marker.to_str().unwrap()
            ),
        ],
        options: None,
        mode: PluginMode::TcpOnly,
    };
    let state = ShadowsocksTransportState::default();
    let options = state
        .plugin_component_options("127.0.0.1", 9999, config)
        .with_process_endpoint(NetworkGeneration(0))
        .unwrap();
    let catalog = ComponentCatalog::default();
    let process = Arc::new(ComponentSlot::from_recipe(
        catalog.allocate_id().unwrap(),
        ResourceGeneration(1),
        ComponentRecipe::from_validated(Arc::new(PluginFactory), Arc::new(options)),
        Default::default(),
        "cold_process",
    ));
    let direct = Arc::new(ComponentSlot::from_recipe(
        catalog.allocate_id().unwrap(),
        ResourceGeneration(1),
        ComponentRecipe::from_validated(
            Arc::new(DirectFactory),
            Arc::new(DirectOptions::validate(Default::default()).unwrap()),
        ),
        Default::default(),
        "actual_direct",
    ));
    let plan = ComponentPlan {
        builtin: direct.clone(),
        borrowing: Default::default(),
        borrowing_revision: 0,
        outbounds: BTreeMap::new(),
        order: vec![process.clone(), direct.clone()],
        started: AtomicBool::new(false),
        deferred_devices: Default::default(),
        device_pass: false,
        failures: Default::default(),
        confirmed_declared_close: Default::default(),
        closing: Default::default(),
        close_evidence: Default::default(),
        shared_identities: Default::default(),
    };
    let host = TestContext::new(plan.order.iter().map(AsRef::as_ref));
    let roots = process.bound_original_budgets().unwrap();
    host.drive(plan.start(deadline())).await.unwrap();
    host.drive(plan.scan_tasks(deadline())).await.unwrap();
    assert!(plan.started.load(Ordering::Acquire) && direct.has_started());
    assert!(process.is_unbuilt_demand_process() && !process.has_started());
    assert!(!marker.exists());
    assert_eq!(roots.process.snapshot().charged_jobs, 0);
    host.drive(process.start_owned(deadline()).unwrap())
        .await
        .unwrap();
    assert!(process.has_started() && !process.is_unbuilt_demand_process());
    assert!(marker.exists());
    assert_eq!(roots.process.snapshot().charged_jobs, 1);
    host.drive(plan.scan_tasks(deadline())).await.unwrap();
    let endpoint = process.ready_process_endpoint(0).unwrap();
    assert!(endpoint.closed().is_none());
    host.drive(plan.shutdown(deadline())).await.unwrap();
    assert_eq!(roots.process.snapshot().charged_jobs, 0);
    assert!(endpoint.closed().unwrap().matches(&endpoint));
}

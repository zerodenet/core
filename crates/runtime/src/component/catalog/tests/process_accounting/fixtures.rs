use super::*;
use crate::component::{
    ComponentRecipe, ComponentRegistry, DirectFactory, DirectOptions, ResourcePreparation,
};
use zero_platform_tokio::contract::component::{
    BuildContext, ComponentFactory, ComponentInstance, ComponentSpec,
};
struct Factory;
struct Options {
    direct: DirectOptions,
    process: zero_core::component::ComponentId,
    replay: zero_core::component::ComponentId,
}
impl ComponentFactory for Factory {
    type Options = Options;
    fn describe(&self, options: &Options) -> ComponentSpec {
        let mut spec = DirectFactory.describe(&options.direct);
        spec.dependencies.extend([options.process, options.replay]);
        spec
    }
    fn build(
        &self,
        options: Arc<Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, zero_platform_tokio::contract::lifecycle::BuildError> {
        DirectFactory.build(Arc::new(options.direct.clone()), context)
    }
}
pub(super) fn catalog(marker: &std::path::Path) -> ComponentCatalog {
    let state = Arc::new(shadowsocks::transport::ShadowsocksTransportState::default());
    let plugin=shadowsocks::validation::PluginConfig {
        command:"python3".into(),
        args:vec!["-c".into(),format!("import os,socket,time; s=socket.socket(); s.bind((os.environ['SS_LOCAL_HOST'],int(os.environ['SS_LOCAL_PORT']))); s.listen(); open({:?},'w').write(str(os.getpid())); time.sleep(60)",marker.to_str().unwrap())],
        options:None,mode:shadowsocks::validation::PluginMode::TcpOnly,
    };
    let mut registry = ComponentRegistry::default();
    registry
        .register_with_resources(
            zero_proxy::component::direct_descriptor(),
            Factory,
            move |_: &zero_config::OutboundConfig, resources: &mut ResourcePreparation<'_>| {
                let options = state
                    .plugin_component_options("127.0.0.1", 9999, plugin.clone())
                    .with_process_endpoint(zero_core::component::NetworkGeneration(0))
                    .unwrap();
                let process = resources.prepare_component_recipe(
                    "actual.process-budget",
                    ComponentRecipe::from_validated(
                        Arc::new(shadowsocks::component::PluginFactory),
                        Arc::new(options),
                    ),
                )?;
                let replay = resources.share(
                    "actual.replay-book",
                    Arc::new(shadowsocks::component::ReplayFactory),
                    Arc::new(state.replay_component_options(
                        shadowsocks::validation::ReplayPolicy::Reject,
                        Some(8),
                    )),
                )?;
                Ok(Options {
                    direct: DirectOptions::validate(Default::default()).unwrap(),
                    process,
                    replay,
                })
            },
        )
        .unwrap();
    ComponentCatalog::with_factories(registry)
}
pub(super) fn dependencies(plan: &Arc<ComponentPlan>) -> (Arc<ComponentSlot>, Arc<ComponentSlot>) {
    let direct = plan.direct(None).unwrap();
    let mut actual = direct
        .recipe
        .spec()
        .dependencies
        .iter()
        .map(|id| plan.component(*id).unwrap());
    (actual.next().unwrap(), actual.next().unwrap())
}

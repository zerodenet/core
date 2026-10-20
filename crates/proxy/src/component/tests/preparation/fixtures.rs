use super::*;
use crate::component::{DirectFactory, DirectOptions};

pub(super) struct Options {
    pub(super) direct: DirectOptions,
    pub(super) dependency: ComponentId,
}
pub(super) struct Factory(Arc<AtomicUsize>, Arc<Mutex<Vec<ComponentInstanceId>>>);
impl ComponentFactory for Factory {
    type Options = Options;
    fn describe(&self, options: &Options) -> ComponentSpec {
        let mut spec = DirectFactory.describe(&options.direct);
        spec.dependencies.push(options.dependency);
        spec
    }
    fn build(
        &self,
        options: Arc<Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        self.1.lock().unwrap().push(
            context
                .dependency(options.dependency)
                .expect("actual resource instance is bound"),
        );
        DirectFactory.build(Arc::new(options.direct.clone()), context)
    }
}

pub(super) fn registry(
    conversions: Arc<AtomicUsize>,
    builds: Arc<AtomicUsize>,
) -> ComponentRegistry<()> {
    registry_with_bindings(conversions, builds, Arc::new(Mutex::new(Vec::new())))
}

pub(super) fn registry_with_bindings(
    conversions: Arc<AtomicUsize>,
    builds: Arc<AtomicUsize>,
    observed: Arc<Mutex<Vec<ComponentInstanceId>>>,
) -> ComponentRegistry<()> {
    let state = Arc::new(::shadowsocks::transport::ShadowsocksTransportState::default());
    let replay = Arc::new(::shadowsocks::component::ReplayFactory);
    let mut registry = ComponentRegistry::default();
    registry
        .register_with_resources(
            crate::adapters::direct::DirectAdapter::default().descriptor(),
            Factory(builds, observed),
            move |_: &(), resources: &mut ResourcePreparation<'_>| {
                conversions.fetch_add(1, Ordering::SeqCst);
                let dependency = resources.share(
                    "shadowsocks.tcp2022.replay",
                    replay.clone(),
                    Arc::new(state.replay_component_options(
                        ::shadowsocks::validation::ReplayPolicy::Reject,
                        Some(2),
                    )),
                )?;
                Ok(Options {
                    direct: DirectOptions::validate(Default::default()).unwrap(),
                    dependency,
                })
            },
        )
        .unwrap();
    registry
}

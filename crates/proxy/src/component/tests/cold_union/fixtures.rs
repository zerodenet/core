use super::*;
use crate::component::{DirectFactory, RegistrationError};
use shadowsocks::{
    component::{ReplayFactory, ReplayOptions, StreamFactory, StreamOptions, TcpPreparation},
    transport::{ShadowsocksOutboundOptionsRef, ShadowsocksTransportState},
    validation::ReplayPolicy,
};
use std::{num::NonZeroUsize, sync::atomic::AtomicUsize};
use zero_config::ConfigError;
use zero_platform_tokio::contract::{
    component::{BuildContext, ComponentFactory, ComponentInstance, ComponentSpec},
    lifecycle::BuildError,
};
use zero_traits::ProtocolMetadata;

#[derive(Default)]
pub(super) struct Controls {
    pub(super) conversions: AtomicUsize,
    pub(super) builds: AtomicUsize,
    pub(super) extra_task: AtomicBool,
    pub(super) conflict: AtomicBool,
    pub(super) fail: AtomicBool,
    pub(super) invalid_dependency: AtomicBool,
    pub(super) capacity: AtomicUsize,
    pub(super) requests: Mutex<Vec<ComponentId>>,
}
struct ReplaySettings {
    actual: ReplayOptions,
    task_limit: NonZeroUsize,
    invalid_dependency: bool,
}
struct Replay(Arc<Controls>);
impl ComponentFactory for Replay {
    type Options = ReplaySettings;
    fn describe(&self, options: &ReplaySettings) -> ComponentSpec {
        let mut spec = ReplayFactory.describe(&options.actual);
        // Keep the same real pool identity and vary another complete setting.
        spec.task_limit = Some(options.task_limit);
        if options.invalid_dependency {
            spec.dependencies.push(ComponentId(u64::MAX));
        }
        spec
    }
    fn build(
        &self,
        options: Arc<ReplaySettings>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        self.0.builds.fetch_add(1, Ordering::SeqCst);
        ReplayFactory.build(Arc::new(options.actual.clone()), context)
    }
}
struct Stream(Arc<Controls>);
impl ComponentFactory for Stream {
    type Options = StreamOptions;
    fn describe(&self, options: &StreamOptions) -> ComponentSpec {
        StreamFactory.describe(options)
    }
    fn build(
        &self,
        options: Arc<StreamOptions>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        self.0.builds.fetch_add(1, Ordering::SeqCst);
        StreamFactory.build(options, context)
    }
}

pub(super) fn catalog(controls: &Arc<Controls>) -> ComponentCatalog {
    let state = Arc::new(ShadowsocksTransportState::default());
    let replay = Arc::new(Replay(controls.clone()));
    let mut factories = ComponentRegistry::default();
    factories
        .register(
            crate::adapters::direct::DirectAdapter::default().descriptor(),
            DirectFactory,
            crate::adapters::direct::factory::decode,
        )
        .unwrap();
    let controls = controls.clone();
    factories
        .register_with_resources(
            shadowsocks::ShadowsocksProtocol.descriptor(),
            Stream(controls.clone()),
            move |input: &OutboundConfig, resources: &mut ResourcePreparation<'_>| {
                controls.conversions.fetch_add(1, Ordering::SeqCst);
                let OutboundProtocolConfig::Shadowsocks {
                    server,
                    port,
                    cipher,
                    password,
                    replay_attack,
                    state_limits,
                    plugin,
                } = &input.protocol
                else {
                    unreachable!("actual Shadowsocks registration")
                };
                let mut limits = *state_limits;
                limits.tcp_replay_capacity = Some(2 + controls.capacity.load(Ordering::SeqCst));
                let prepared = state
                    .prepare_tcp(TcpPreparation {
                        server,
                        port: *port,
                        protocol: ShadowsocksOutboundOptionsRef { cipher, password },
                        replay_policy: *replay_attack,
                        limits,
                        plugin: plugin.as_ref(),
                        dial: input.dial.to_policy(),
                    })
                    .map_err(options_error)?;
                let actual = prepared.replay_options().unwrap().clone();
                assert!(matches!(*replay_attack, ReplayPolicy::Reject));
                let extra = controls.extra_task.load(Ordering::SeqCst)
                    || (input.tag == "b" && controls.conflict.load(Ordering::SeqCst));
                let dependency = resources.share(
                    "actual.assembly.shadowsocks.replay",
                    replay.clone(),
                    Arc::new(ReplaySettings {
                        actual,
                        task_limit: NonZeroUsize::new(1 + usize::from(extra)).unwrap(),
                        invalid_dependency: controls.invalid_dependency.load(Ordering::SeqCst),
                    }),
                )?;
                controls.requests.lock().unwrap().push(dependency);
                if input.tag == "b" && controls.fail.load(Ordering::SeqCst) {
                    return Err(options_error("selected consumer validation failed"));
                }
                prepared.bind(Some(dependency)).map_err(options_error)
            },
        )
        .unwrap();
    ComponentCatalog::with_factories(factories)
}
fn options_error(error: impl std::fmt::Display) -> RegistrationError {
    RegistrationError::Options(ConfigError::InvalidOutbound(error.to_string()))
}

pub(super) fn config() -> Arc<RuntimeConfig> {
    Arc::new(
        RuntimeConfig::parse(
            r#"{"outbounds":[
              {"tag":"a","protocol":{"type":"shadowsocks","server":"127.0.0.1","port":10001,"cipher":"2022-blake3-aes-128-gcm","replay_attack":"reject","password":"AAAAAAAAAAAAAAAAAAAAAA=="}},
              {"tag":"b","protocol":{"type":"shadowsocks","server":"127.0.0.1","port":10002,"cipher":"2022-blake3-aes-128-gcm","replay_attack":"reject","password":"AAAAAAAAAAAAAAAAAAAAAA=="}},
              {"tag":"separate","protocol":{"type":"direct"}}
            ],"route":{"final":{"type":"direct"}}}"#,
        )
        .unwrap(),
    )
}
pub(super) fn resource(plan: &ComponentPlan) -> &Arc<ComponentSlot> {
    let resources: Vec<_> = plan
        .order
        .iter()
        .filter(|slot| slot.recipe.spec().roles.is_empty())
        .collect();
    assert_eq!(resources.len(), 1);
    resources[0]
}

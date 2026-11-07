pub(crate) mod inbound_service;
use std::future::Future;
use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;

use tokio::sync::oneshot;
use tracing::{info, warn};
use zero_config::RuntimeConfig;
use zero_dns::DnsSystem;
use zero_engine::{Engine, EngineError};

use crate::inventory::ProtocolInventory;
use crate::protocol_registry::TcpRuntimeServices;
mod preparation;
pub use preparation::ProxyPreparation;

pub(crate) mod current_services;
#[cfg(feature = "udp-runtime")]
pub(crate) mod datagram_udp;
mod diagnostic_service;
mod dns_outbound;
mod handle;
pub(crate) mod http_redirect;
pub(crate) mod inbound_operation;
pub(crate) mod inbound_owners;
#[cfg(test)]
pub(crate) mod inbound_route;
pub(crate) mod listener_loop;
mod listeners;
#[cfg(feature = "managed-stream-runtime")]
pub(crate) mod mux_session;
#[cfg(feature = "managed-stream-runtime")]
pub(crate) mod mux_tcp;
#[cfg(feature = "managed-stream-runtime")]
pub(crate) mod mux_udp;
#[cfg(feature = "raw-ip-runtime")]
pub(crate) mod network_graph;
pub(crate) mod orchestration;
pub(crate) mod outbound_probe;
mod packet_close_owner;
#[cfg(feature = "raw-ip-runtime")]
pub(crate) mod packet_flow;
#[cfg(feature = "raw-ip-runtime")]
pub(crate) mod packet_route;
#[cfg(feature = "managed-stream-runtime")]
pub(crate) mod packet_session_udp;
mod passive_relay_health;
pub(crate) use passive_relay_health::classify_channel_establishment_failure;
pub(crate) mod path;
pub(crate) mod pipe;
pub(crate) mod principal_rate_limit;
#[cfg(feature = "raw-ip-runtime")]
pub(crate) mod raw_ip;
mod relay_failure;
pub(crate) mod route_runtime;
mod running;
pub(crate) mod sniff;
// Direct and standard inbound streams also recover DNS targets.
mod target;
#[cfg(feature = "raw-ip-runtime")]
pub(crate) use target::resolve_packet_targets;
pub(crate) mod tcp_dispatch;
pub(crate) mod tcp_ingress;
#[cfg(any(
    feature = "tcp-tunnel-runtime",
    feature = "tcp-session-runtime",
    feature = "managed-stream-runtime"
))]
pub(crate) mod transport_leaf;
#[cfg(feature = "udp-runtime")]
pub(crate) mod udp_delivery;
#[cfg(feature = "udp-runtime")]
pub(crate) mod udp_dispatch;
#[cfg(feature = "udp-runtime")]
pub(crate) mod udp_flow;
#[cfg(feature = "udp-runtime")]
pub(crate) mod udp_ingress;
#[cfg(feature = "udp-runtime")]
pub(crate) mod udp_socket;

pub use handle::{
    ConfigApplyReconciler, ConfigReconcileResult, ProxyCapabilitiesQuery, ProxyControlQuery,
    ProxyEndpointQuery, ProxyEndpointQueryError, ProxyEndpointQueryGuard, ProxyHandle,
    ProxyHealthQuery,
};
mod reload_ack;
use reload_ack::PendingReloadAck;
pub(crate) type RuntimeHost = zero_runtime::RuntimeHost<PendingReloadAck>;
#[cfg(test)]
#[path = "runtime/tests/host.rs"]
mod host_tests;
pub use running::RunningProxy;

#[derive(Debug, Clone)]
pub struct Proxy {
    engine: Engine,
    pub(crate) config: Arc<RuntimeConfig>,
    pub(crate) resolver: Arc<DnsSystem>,
    pub(crate) protocols: ProtocolInventory,
    pub(crate) egress_interface: zero_platform_tokio::EgressInterfaceControl,
    pub(crate) tun_control: Arc<std::sync::Mutex<Option<TunControl>>>,
    pub(crate) tun_info: Arc<std::sync::Mutex<Option<TunInfo>>>,
    pub(crate) tun_last_error: Arc<std::sync::Mutex<Option<String>>>,
    pub(crate) tun_operation_lock: Arc<tokio::sync::Mutex<()>>,
    pub(crate) configured_tun_failures: tokio::sync::broadcast::Sender<String>,
    orchestration_ready: tokio::sync::watch::Sender<bool>,
    host: Arc<RuntimeHost>,
    packet_close_owner: Arc<packet_close_owner::PacketCloseOwner>,
    statistics: Arc<orchestration::statistics::Sampler>,
    inbound_owners: Arc<inbound_owners::InboundOwnerRegistry>,
    retired_datagrams: Arc<crate::component::datagram_retirement::RetiredDatagrams>,
    principal_rate_limits: principal_rate_limit::PrincipalRateLimitRegistry,
    current_services: Arc<current_services::CurrentServiceSource>,
}

#[derive(Debug, Clone)]
pub(crate) struct TunInfo {
    pub id: u64,
    pub name: String,
    pub addr: String,
    pub addresses: Vec<String>,
    pub mtu: u16,
    pub tag: String,
    pub auto_route: bool,
    pub include_cidrs: Vec<ipnet::IpNet>,
    pub exclude_cidrs: Vec<ipnet::IpNet>,
    pub dual_stack: bool,
    pub strict_route: bool,
    pub dns_hijack: bool,
    pub dns_hijacked_queries: Arc<std::sync::atomic::AtomicU64>,
    pub healthy: bool,
    pub last_error: Option<String>,
    pub egress_interface: Option<String>,
    pub egress_interface_v4: Option<String>,
    pub egress_interface_v6: Option<String>,
    pub route_exclusions: Vec<std::net::IpAddr>,
    pub managed_config: Option<zero_config::TunConfig>,
}

#[derive(Debug)]
pub(crate) struct TunControl {
    pub id: u64,
    pub shutdown: tokio::sync::watch::Sender<bool>,
    pub completion: Arc<crate::inbound::tun::completion::TunCompletion>,
    #[cfg(feature = "raw-ip-runtime")]
    pub budget_recovery: Option<Arc<crate::inbound::tun::budget::TunBudgetRecovery>>,
    pub route_recovery: Option<tokio::sync::mpsc::Sender<oneshot::Sender<Result<(), String>>>>,
}

impl Proxy {
    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) fn system_tun_source_registry(&self) -> Arc<inbound_owners::InboundOwnerRegistry> {
        self.inbound_owners.clone()
    }
    pub fn new(config: RuntimeConfig) -> Result<Self, EngineError> {
        Self::from_engine(Engine::new(config)?)
    }

    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, EngineError> {
        // This public convenience entry remains load-only: reading a document
        // must not grant persistence to its path.
        let source = Arc::new(zero_runtime::configuration::ConfigurationSource::default());
        source.verify_initial_recovery(path.as_ref())?;
        let config = source.load_initial_configuration(path)?;
        let prepared =
            zero_runtime::configuration::PreparedConfiguration::prepare_startup(config, source)?;
        Self::from_engine(Engine::from_prepared(prepared)?)
    }

    pub fn from_engine(engine: Engine) -> Result<Self, EngineError> {
        // Ordinary registration stays closed until the complete native carrier
        // matrix and compatibility regressions qualify this exact assembly.
        Self::from_engine_with_background(engine, false)
    }

    pub(crate) fn from_engine_with_background(
        engine: Engine,
        native_background: bool,
    ) -> Result<Self, EngineError> {
        Self::from_preparation(engine, ProxyPreparation::with_background(native_background))
    }

    /// Complete the very same cold Host used for initial rule downloads.
    /// The preparation is consumed once; no second inventory or pool is built.
    pub fn from_preparation(
        engine: Engine,
        preparation: ProxyPreparation,
    ) -> Result<Self, EngineError> {
        let ProxyPreparation {
            egress_interface,
            node_network,
            background,
            protocols,
        } = preparation;
        let config = engine.config();
        preparation::install_original_limit(&protocols, &config.runtime)?;
        preparation::install_control_limit(&protocols, &config.runtime)?;
        protocols.validate_config(&config)?;
        protocols.validate_dial_environment(&config)?;
        protocols.validate_components(config.clone())?;
        let dns_dispatch = engine.runtime_snapshot().dns_dispatch();
        let fake_ip_state_path = config
            .source_dir()
            .map(zero_dns::default_fake_ip_state_path);
        let dns = DnsSystem::build_with_native_queries(
            config.runtime.dns.as_ref(),
            dns_dispatch,
            egress_interface.clone(),
            fake_ip_state_path,
            node_network.query_dns.clone(),
        )
        .map_err(EngineError::Io)?;
        let (orchestration_ready, _) = tokio::sync::watch::channel(false);
        let (configured_tun_failures, _) = tokio::sync::broadcast::channel(16);
        let resolver = Arc::new(
            dns.with_native_query_queries(node_network.query_dns.clone())
                .with_native_node_queries(node_network.native_dns.clone()),
        );
        protocols
            .runtime_host()
            .components()
            .install_node_dns_source(resolver.native_node_service())
            .map_err(|source| EngineError::Io(std::io::Error::other(source)))?;
        let principal_rate_limits = principal_rate_limit::PrincipalRateLimitRegistry::default();
        // Independent host limit for active + retiring native associations.
        // This is not the V1 packet queue capacity or a legacy session budget.
        const RETIRED_DATAGRAM_LIMIT: usize = 4096;
        let retired_datagrams = Arc::new(
            crate::component::datagram_retirement::RetiredDatagrams::new(
                std::num::NonZeroUsize::new(RETIRED_DATAGRAM_LIMIT).unwrap(),
            ),
        );
        let component_tasks = protocols
            .runtime_host()
            .original_budgets()
            .component_tasks
            .clone();
        let packet_close_owner = Arc::new(packet_close_owner::PacketCloseOwner::new(
            engine.packet_route_capacity(),
            component_tasks.clone(),
        ));
        let inbound_owners = Arc::new(inbound_owners::InboundOwnerRegistry::default());
        let current_services = Arc::new(current_services::CurrentServiceSource::new(
            engine.clone(),
            &resolver,
            &protocols,
            egress_interface.clone(),
            principal_rate_limits.clone(),
            &retired_datagrams,
            &inbound_owners,
        ));
        node_network.install(&resolver, Arc::downgrade(&current_services));
        let proxy = Self {
            config,
            engine,
            resolver: resolver.clone(),
            host: protocols.runtime_host(),
            protocols,
            egress_interface,
            tun_control: Arc::new(std::sync::Mutex::new(None)),
            tun_info: Arc::new(std::sync::Mutex::new(None)),
            tun_last_error: Arc::new(std::sync::Mutex::new(None)),
            tun_operation_lock: Arc::new(tokio::sync::Mutex::new(())),
            configured_tun_failures,
            orchestration_ready,
            packet_close_owner,
            statistics: Arc::new(orchestration::statistics::Sampler::with_task_budget(
                component_tasks,
            )),
            inbound_owners,
            retired_datagrams: retired_datagrams.clone(),
            principal_rate_limits: principal_rate_limits.clone(),
            current_services,
        };
        resolver.set_outbound_connector(Arc::new(dns_outbound::ProxyDnsOutboundConnector::new(
            proxy.current_service_source(),
        )));
        Ok(proxy)
    }

    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    pub(crate) fn current_service_source(
        &self,
    ) -> std::sync::Weak<current_services::CurrentServiceSource> {
        Arc::downgrade(&self.current_services)
    }

    /// Shared physical-egress authority for application-owned network services.
    ///
    /// Connector webhook delivery is created by the root application rather
    /// than the proxy runtime, but its sockets must observe the same TUN
    /// underlay selection and generation changes as proxy and DNS sockets.
    pub fn egress_interface_control(&self) -> zero_platform_tokio::EgressInterfaceControl {
        self.egress_interface.clone()
    }

    /// Transfer the existing Host's concrete custody root to application-owned
    /// event network resources, together with one identity issued by its catalog.
    /// This does not create a host, pool, task authority or native resource.
    /// The same cold Host book used by embedded and production control calls.
    pub fn application_control_budget(&self) -> Arc<zero_runtime::configuration::ControlBudget> {
        self.host.control_budget()
    }
    pub fn embedded_command_owner(&self) -> Arc<zero_runtime::EmbeddedCommandOwner> {
        self.host.embedded_commands().clone()
    }
    pub fn application_network_originals(
        &self,
    ) -> Result<
        (
            zero_core::component::ComponentInstanceId,
            Arc<zero_platform_tokio::controlled_carrier::CarrierOriginalTotals>,
        ),
        EngineError,
    > {
        Ok((
            self.protocols.allocate_resource_identity()?,
            self.host.original_budgets().carrier.clone(),
        ))
    }

    #[cfg(test)]
    pub(crate) fn orchestration_readiness(&self) -> tokio::sync::watch::Receiver<bool> {
        self.orchestration_ready.subscribe()
    }
    pub(crate) fn mark_orchestration_ready(&self) {
        self.orchestration_ready.send_replace(true);
    }

    pub(crate) fn complete_reload(
        &self,
        expected: &Arc<zero_engine::EngineRuntimeSnapshot>,
        result: Result<(), String>,
    ) {
        let pending = {
            let mut pending = self
                .host
                .reload_ack()
                .lock()
                .expect("reload ack lock poisoned");
            if pending
                .as_ref()
                .is_some_and(|pending| Arc::ptr_eq(&pending.expected, expected))
            {
                pending.take()
            } else {
                None
            }
        };
        if let Some(pending) = pending {
            let _ = pending.sender.send(result);
        }
    }

    pub(crate) fn pending_configuration_snapshot(
        &self,
    ) -> Option<Arc<zero_engine::EngineRuntimeSnapshot>> {
        self.host
            .reload_ack()
            .lock()
            .expect("reload ack lock poisoned")
            .as_ref()
            .map(|pending| pending.expected.clone())
    }

    pub(crate) fn pending_reload_snapshot(
        &self,
        expected: &Arc<zero_engine::EngineRuntimeSnapshot>,
    ) -> Option<Arc<zero_engine::EngineRuntimeSnapshot>> {
        self.host
            .reload_ack()
            .lock()
            .expect("reload ack lock poisoned")
            .as_ref()
            .filter(|pending| Arc::ptr_eq(&pending.expected, expected))
            .map(|pending| pending.previous.clone())
    }

    pub(crate) fn pending_reload_matches(
        &self,
        expected: &Arc<zero_engine::EngineRuntimeSnapshot>,
    ) -> bool {
        self.host
            .reload_ack()
            .lock()
            .expect("reload ack lock poisoned")
            .as_ref()
            .is_some_and(|pending| Arc::ptr_eq(&pending.expected, expected))
    }

    pub(crate) fn pending_reload_compensates(
        &self,
        expected: &Arc<zero_engine::EngineRuntimeSnapshot>,
    ) -> bool {
        self.host
            .reload_ack()
            .lock()
            .expect("reload ack lock poisoned")
            .as_ref()
            .is_some_and(|pending| pending.compensating && Arc::ptr_eq(&pending.expected, expected))
    }

    pub(crate) fn tcp_runtime_services(&self) -> TcpRuntimeServices {
        self.tcp_runtime_services_for_snapshot(self.engine.runtime_snapshot())
    }

    /// Select only an actually owned snapshot for this exact accepted config.
    /// Source startup must retain its pending publication gate before the main
    /// loop refreshes ordinary routing from the current committed snapshot.
    pub(crate) fn tcp_runtime_services_for_owned_config(
        &self,
        config: &Arc<RuntimeConfig>,
    ) -> Result<TcpRuntimeServices, zero_engine::EngineError> {
        let snapshot = self
            .pending_configuration_snapshot()
            .filter(|snapshot| Arc::ptr_eq(snapshot.config(), config))
            .or_else(|| {
                let current = self.engine.runtime_snapshot();
                Arc::ptr_eq(current.config(), config).then_some(current)
            })
            .ok_or_else(|| {
                std::io::Error::other("Source configuration has no matching owned snapshot")
            })?;
        Ok(self.tcp_runtime_services_for_snapshot(snapshot))
    }

    pub(crate) fn tcp_runtime_services_for_snapshot(
        &self,
        snapshot: Arc<zero_engine::EngineRuntimeSnapshot>,
    ) -> TcpRuntimeServices {
        TcpRuntimeServices::new(
            self.engine().clone(),
            snapshot,
            self.resolver.clone(),
            self.protocols.clone(),
            self.egress_interface.clone(),
            self.principal_rate_limits.clone(),
            self.retired_datagrams.clone(),
        )
        .with_current_service_source(self.current_service_source())
    }

    #[cfg(all(test, feature = "udp-runtime"))]
    pub(crate) fn udp_runtime_services(&self) -> crate::protocol_registry::UdpRuntimeServices {
        crate::protocol_registry::UdpRuntimeServices::new(self.tcp_runtime_services())
    }

    pub fn with_udp_upstream_idle_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.engine = self.engine.with_udp_upstream_idle_timeout(timeout);
        self
    }

    pub fn into_engine(self) -> Engine {
        self.engine
    }

    pub fn spawn(&self) -> RunningProxy {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let proxy = self.clone();
        let task = tokio::spawn(async move {
            proxy
                .run_until(async {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        RunningProxy {
            proxy: self.clone(),
            shutdown: Some(shutdown_tx),
            task,
        }
    }

    pub async fn run(&self) -> Result<(), EngineError> {
        self.run_until(async {
            match tokio::signal::ctrl_c().await {
                Ok(()) => info!("shutdown signal received"),
                Err(error) => warn!(error = %error, "failed to listen for ctrl-c; stopping proxy"),
            }
        })
        .await
    }

    pub async fn run_until<F>(&self, shutdown: F) -> Result<(), EngineError>
    where
        F: Future<Output = ()> + Send,
    {
        orchestration::run_until(self, shutdown)
            .await
            .map_err(inbound_owners::preserve_io_kind)
    }

    pub async fn probe_outbound_single(
        &self,
        target_tag: &str,
        url: &str,
    ) -> Result<u64, EngineError> {
        let request = outbound_probe::OutboundProbeRequest::parse(url)
            .map_err(|error| EngineError::Io(std::io::Error::other(error)))?;
        let proxy = self.clone();
        let target_tag = target_tag.to_owned();
        self.with_diagnostic_service(async move {
            outbound_probe::OutboundProbeRuntime::new(proxy.tcp_runtime_services())
                .probe_target_tag(&target_tag, &request)
                .await
                .map_err(|error| EngineError::Io(std::io::Error::other(error)))
        })
        .await?
    }
}

impl Deref for Proxy {
    type Target = Engine;

    fn deref(&self) -> &Self::Target {
        &self.engine
    }
}

pub(crate) mod traffic_io;

#[cfg(all(test, feature = "vless", feature = "vmess", feature = "trojan"))]
#[path = "runtime/tests/mux_prefix.rs"]
mod mux_prefix;
#[cfg(all(test, feature = "vless", feature = "vmess", feature = "trojan"))]
pub(crate) use mux_prefix::MuxPrefix;

#[cfg(all(test, feature = "managed-datagram-runtime"))]
#[path = "runtime/tests/standard_listener.rs"]
pub(crate) mod test_listener;

#[cfg(all(test, feature = "hysteria2"))]
#[path = "runtime/tests/hysteria2.rs"]
mod hysteria2_tests;

#[cfg(all(test, feature = "shadowsocks", unix))]
#[path = "runtime/tests/ss_plugin.rs"]
mod ss_plugin_tests;

#[cfg(all(test, feature = "wireguard"))]
#[path = "runtime/tests/wg_packet.rs"]
mod wg_packet_tests;

//! TUN inbound lifecycle and proxy-kernel integration.

#[cfg(feature = "raw-ip-runtime")]
pub(crate) mod budget;
pub(crate) mod completion;
mod config;
#[cfg(feature = "raw-ip-runtime")]
pub(crate) mod exit;
#[cfg(feature = "raw-ip-runtime")]
mod managed;
#[cfg(all(test, feature = "raw-ip-runtime"))]
mod packet;
#[cfg(feature = "raw-ip-runtime")]
mod policy;
mod recovery;
#[cfg(feature = "raw-ip-runtime")]
pub(crate) use policy::TunIngressOptions;
mod routes;
#[cfg(any(test, feature = "raw-ip-runtime"))]
mod runtime;
#[cfg(feature = "raw-ip-runtime")]
pub(crate) use runtime::serve_dns_tcp_owned;
#[cfg(feature = "raw-ip-runtime")]
pub(crate) use sniff::sniff_tcp_target;
#[cfg(feature = "raw-ip-runtime")]
pub(crate) const MAX_TCP_DNS_CONNECTIONS: usize = runtime::MAX_CONCURRENT_DNS_CONNECTIONS;
mod sniff;
#[cfg(feature = "raw-ip-runtime")]
pub(crate) mod source;
mod statistics;
#[cfg(feature = "udp-runtime")]
pub(crate) mod udp;

use std::io;
use std::net::IpAddr;
#[cfg(feature = "raw-ip-runtime")]
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(feature = "raw-ip-runtime")]
use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, info};
use zero_engine::EngineError;

use crate::runtime::{Proxy, TunControl, TunInfo};
use config::parse_interface_addresses;

#[cfg(feature = "raw-ip-runtime")]
static NEXT_TUN_ID: AtomicU64 = AtomicU64::new(1);
const MAX_CAPTURE_ROUTE_PREFIXES: usize = 512;

#[derive(Clone, Debug)]
pub struct TunRuntimeOptions {
    pub auto_route: bool,
    pub include_cidrs: Vec<ipnet::IpNet>,
    pub exclude_cidrs: Vec<ipnet::IpNet>,
    pub dual_stack: bool,
    pub strict_route: bool,
    pub dns_hijack: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct TunInterfaceOptions<'a> {
    pub name: Option<&'a str>,
    pub addr: &'a str,
    pub mask: &'a str,
    pub secondary_addr: Option<&'a str>,
}

#[derive(Default)]
struct TunStartPreparation {
    #[cfg(feature = "raw-ip-runtime")]
    config: Option<Arc<zero_config::RuntimeConfig>>,
    #[cfg(feature = "raw-ip-runtime")]
    recovery: Option<Arc<budget::TunBudgetRecovery>>,
}

struct TunStartSpec<'a> {
    name: Option<&'a str>,
    addr: &'a str,
    mask: &'a str,
    secondary_addr: Option<&'a str>,
    mtu: u16,
    tag: &'a str,
    options: TunRuntimeOptions,
    managed_config: Option<zero_config::TunConfig>,
    prepared_network: Option<PreparedTunNetwork>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PreparedTunNetwork {
    dns_hijack: bool,
    route_exclusions: Vec<IpAddr>,
}

fn tun_route_exclusion_required(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => {
            !address.is_unspecified()
                && !address.is_loopback()
                && !address.is_multicast()
                && !address.is_broadcast()
                && !address.is_link_local()
        }
        IpAddr::V6(address) => {
            !address.is_unspecified()
                && !address.is_loopback()
                && !address.is_multicast()
                && !address.is_unicast_link_local()
        }
    }
}

fn configured_tun_is_current(
    current: &TunInfo,
    desired: &zero_config::TunConfig,
    network_mtu: u16,
    prepared: &PreparedTunNetwork,
) -> bool {
    current.managed_config.as_ref() == Some(desired)
        && current.mtu == desired.effective_mtu(network_mtu)
        && current.dns_hijack == prepared.dns_hijack
        && current.route_exclusions == prepared.route_exclusions
}

impl Proxy {
    pub async fn start_tun(
        &self,
        interface: TunInterfaceOptions<'_>,
        mtu: u16,
        tag: &str,
        options: TunRuntimeOptions,
    ) -> Result<(), EngineError> {
        let TunInterfaceOptions {
            name,
            addr,
            mask,
            secondary_addr,
        } = interface;
        self.start_tun_internal(TunStartSpec {
            name,
            addr,
            mask,
            secondary_addr,
            mtu,
            tag,
            options,
            managed_config: None,
            prepared_network: None,
        })
        .await
    }

    async fn start_tun_internal(&self, spec: TunStartSpec<'_>) -> Result<(), EngineError> {
        let _operation = self.tun_operation_lock.lock().await;
        self.start_tun_prepared(spec, TunStartPreparation::default())
            .await
    }

    async fn start_tun_prepared(
        &self,
        spec: TunStartSpec<'_>,
        prepared: TunStartPreparation,
    ) -> Result<(), EngineError> {
        #[cfg(not(feature = "raw-ip-runtime"))]
        let _ = prepared;
        let TunStartSpec {
            name,
            addr,
            mask,
            secondary_addr,
            mtu,
            tag,
            options,
            managed_config,
            prepared_network,
        } = spec;
        let TunRuntimeOptions {
            auto_route,
            include_cidrs,
            exclude_cidrs,
            dual_stack,
            strict_route,
            dns_hijack,
        } = options;
        if (!include_cidrs.is_empty() || !exclude_cidrs.is_empty()) && !auto_route {
            return Err(EngineError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "TUN include/exclude CIDRs require automatic routes",
            )));
        }
        if include_cidrs.len() > 128 {
            return Err(EngineError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "TUN include CIDRs support at most 128 entries",
            )));
        }
        if exclude_cidrs.len() > 128 {
            return Err(EngineError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "TUN exclude CIDRs support at most 128 entries",
            )));
        }
        let unique_include_cidrs = include_cidrs
            .iter()
            .collect::<std::collections::HashSet<_>>();
        if unique_include_cidrs.len() != include_cidrs.len() {
            return Err(EngineError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "TUN include CIDRs must not contain duplicates",
            )));
        }
        let unique_exclude_cidrs = exclude_cidrs
            .iter()
            .collect::<std::collections::HashSet<_>>();
        if unique_exclude_cidrs.len() != exclude_cidrs.len() {
            return Err(EngineError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "TUN exclude CIDRs must not contain duplicates",
            )));
        }
        if !cfg!(feature = "udp-runtime") {
            return Err(EngineError::Io(io::Error::new(
                io::ErrorKind::Unsupported,
                "TUN requires the zero-proxy `udp-runtime` feature",
            )));
        }
        let current = self.tun_control.lock().unwrap().as_ref().map(|control| {
            (control.completion.clone(), {
                #[cfg(feature = "raw-ip-runtime")]
                {
                    control.budget_recovery.clone()
                }
                #[cfg(not(feature = "raw-ip-runtime"))]
                {
                    None::<()>
                }
            })
        });
        if let Some((completion, recovery)) = current {
            #[cfg(feature = "raw-ip-runtime")]
            let replacing = prepared.recovery.as_ref().is_some_and(|expected| {
                recovery
                    .as_ref()
                    .is_some_and(|actual| Arc::ptr_eq(actual, expected))
            }) && completion.resources_complete().await;
            #[cfg(not(feature = "raw-ip-runtime"))]
            let replacing = {
                let _ = (completion, recovery);
                false
            };
            if !replacing {
                return Err(
                    io::Error::new(io::ErrorKind::AlreadyExists, "TUN is already running").into(),
                );
            }
        } else {
            #[cfg(feature = "raw-ip-runtime")]
            if prepared.recovery.is_some() {
                return Err(
                    io::Error::other("System TUN budget recovery lost its actual control").into(),
                );
            }
        }
        if mtu < 576 {
            return Err(EngineError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "TUN MTU must be at least 576",
            )));
        }
        let interface_addresses =
            parse_interface_addresses(addr, mask, secondary_addr, dual_stack)?;
        if let Some(cidr) = include_cidrs.iter().chain(&exclude_cidrs).find(|cidr| {
            !interface_addresses
                .iter()
                .any(|address| address.address.is_ipv6() == cidr.addr().is_ipv6())
        }) {
            return Err(EngineError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("TUN capture CIDR `{cidr}` has no configured interface address family"),
            )));
        }
        let compiled_route_count = interface_addresses
            .iter()
            .map(|address| {
                zero_tun::capture_route_prefixes_with_exclusions(
                    address.address,
                    &include_cidrs,
                    &exclude_cidrs,
                )
                .len()
            })
            .sum::<usize>();
        if compiled_route_count > MAX_CAPTURE_ROUTE_PREFIXES {
            return Err(EngineError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "TUN capture plan expands to {compiled_route_count} routes; maximum is {MAX_CAPTURE_ROUTE_PREFIXES}"
                ),
            )));
        }
        let route_addresses = interface_addresses
            .iter()
            .filter(|address| {
                !zero_tun::capture_route_prefixes_with_exclusions(
                    address.address,
                    &include_cidrs,
                    &exclude_cidrs,
                )
                .is_empty()
            })
            .map(|address| (address.address, address.netmask))
            .collect::<Vec<_>>();
        if auto_route && route_addresses.is_empty() {
            return Err(EngineError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "TUN include/exclude CIDRs leave no captured route for a configured interface family",
            )));
        }
        let mut tun_owned_addresses = interface_addresses
            .iter()
            .map(|address| address.address)
            .collect::<Vec<_>>();
        tun_owned_addresses.extend(
            interface_addresses
                .iter()
                .filter_map(|address| next_ip(address.address)),
        );
        // Declarative TUN/Fake-IP pairs are cross-validated together before
        // the runtime config is staged. Command-driven TUN starts have no such
        // candidate config, so they must still validate against the committed
        // resolver here.
        if managed_config.is_none() {
            if let Some(conflict) = self.resolver.fake_ip_conflict(&tun_owned_addresses) {
                return Err(EngineError::Io(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "Fake-IP pool overlaps TUN-owned address `{conflict}`; choose a non-overlapping DNS fake-IP CIDR"
                    ),
                )));
            }
        }
        #[cfg(feature = "raw-ip-runtime")]
        {
            let _ = prepared_network;
            self.start_managed_tun(
                managed::Start {
                    name: name.map(str::to_owned),
                    raw_addr: addr.to_owned(),
                    addresses: interface_addresses,
                    route_addresses,
                    mtu,
                    tag: tag.to_owned(),
                    options: TunRuntimeOptions {
                        auto_route,
                        include_cidrs,
                        exclude_cidrs,
                        dual_stack,
                        strict_route,
                        dns_hijack,
                    },
                    managed_config,
                    dns_hijacked_queries: Arc::new(AtomicU64::new(0)),
                },
                prepared.config.unwrap_or_else(|| self.engine().config()),
                prepared.recovery,
            )
            .await
        }
        #[cfg(not(feature = "raw-ip-runtime"))]
        {
            // udp-runtime implies raw-ip-runtime. No legal enabled TUN build
            // can reach a second native creator or private stack executor.
            let _ = (prepared_network, route_addresses, interface_addresses);
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "TUN requires the zero-proxy `udp-runtime` feature",
            )
            .into())
        }
    }

    pub(crate) async fn reconcile_configured_tun(
        &self,
        desired: Option<&zero_config::TunConfig>,
        network_mtu: u16,
    ) -> Result<(), EngineError> {
        self.reconcile_configured_tun_candidate(desired, network_mtu, None).await
    }
    pub(crate) async fn reconcile_configured_tun_for_config(
        &self,
        config: Arc<zero_config::RuntimeConfig>,
    ) -> Result<(), EngineError> {
        self.reconcile_configured_tun_candidate(
            config.runtime.tun.as_ref(), config.runtime.network.mtu, Some(config.clone()),
        ).await
    }
    async fn reconcile_configured_tun_candidate(
        &self,
        desired: Option<&zero_config::TunConfig>,
        network_mtu: u16,
        candidate: Option<Arc<zero_config::RuntimeConfig>>,
    ) -> Result<(), EngineError> {
        #[cfg(feature = "raw-ip-runtime")]
        if self.has_tun_packet_budget_recovery() {
            let config = candidate.clone().unwrap_or_else(|| self.engine().config());
            if config.runtime.tun.as_ref() != desired || config.runtime.network.mtu != network_mtu {
                return Err(io::Error::other(
                    "System TUN recovery requires the exact candidate snapshot",
                )
                .into());
            }
            return self.resume_tun_after_packet_budget(config).await;
        }
        let current = self.tun_info.lock().unwrap().clone();
        #[cfg(feature = "raw-ip-runtime")]
        let prepared_network = if let (Some(current), Some(desired)) = (&current, desired) {
            parse_interface_addresses(
                &desired.addr,
                &desired.mask,
                desired.secondary_addr.as_deref(),
                desired.dual_stack,
            )?;
            if current.managed_config.as_ref() == Some(desired)
                && current.mtu == desired.effective_mtu(network_mtu)
            {
                Some(self.prepare_live_tun_network(desired).await?)
            } else {
                None // New Source discovers only after confirmed old retirement.
            }
        } else {
            None
        };
        #[cfg(not(feature = "raw-ip-runtime"))]
        let prepared_network = if let Some(desired) = desired {
            parse_interface_addresses(
                &desired.addr,
                &desired.mask,
                desired.secondary_addr.as_deref(),
                desired.dual_stack,
            )?;
            Some(
                self.prepare_tun_network(
                    desired.auto_route,
                    desired.dns_hijack,
                    desired.strict_route,
                )
                .await?,
            )
        } else {
            None
        };
        if let (Some(current), Some(desired)) = (&current, desired) {
            if prepared_network.as_ref().is_some_and(|prepared| {
                configured_tun_is_current(current, desired, network_mtu, prepared)
            }) {
                return Ok(());
            }
        }

        // An exited body retains the original task/native control until this
        // next supervisor pass actually joins and closes it.
        if current.is_none() && self.tun_control.lock().unwrap().is_some() {
            self.stop_tun_internal(true).await?;
        }
        if desired.is_none() {
            if current
                .as_ref()
                .is_some_and(|current| current.managed_config.is_some())
            {
                self.stop_tun_internal(true).await?;
            }
            return Ok(());
        }

        if let Some(current) = current {
            if current.managed_config.is_none() {
                return Err(EngineError::Io(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "cannot enable configured TUN while a command-managed TUN is running",
                )));
            }
            self.stop_tun_internal(true).await?;
        }

        let desired = desired.expect("configured TUN desired state checked above");
        self.start_tun_prepared(TunStartSpec {
            name: desired.name.as_deref(),
            addr: &desired.addr,
            mask: &desired.mask,
            secondary_addr: desired.secondary_addr.as_deref(),
            mtu: desired.effective_mtu(network_mtu),
            tag: &desired.tag,
            options: TunRuntimeOptions {
                auto_route: desired.auto_route,
                include_cidrs: desired.include_cidrs.clone(),
                exclude_cidrs: desired.exclude_cidrs.clone(),
                dual_stack: desired.dual_stack,
                strict_route: desired.strict_route,
                dns_hijack: desired.dns_hijack,
            },
            managed_config: Some(desired.clone()),
            prepared_network,
        }, TunStartPreparation {
            #[cfg(feature = "raw-ip-runtime")]
            config: candidate,
            #[cfg(feature = "raw-ip-runtime")]
            recovery: None,
        })
        .await
    }

    pub(crate) async fn stop_tun_if_running(&self) -> Result<(), EngineError> {
        if self.tun_control.lock().unwrap().is_none() {
            return Ok(());
        }
        self.stop_tun_internal(true).await
    }

    #[cfg(any(test, not(feature = "raw-ip-runtime")))]
    async fn prepare_tun_network(
        &self,
        auto_route: bool,
        dns_hijack: bool,
        strict_route: bool,
    ) -> Result<PreparedTunNetwork, EngineError> {
        let (dns_hijack, dns_route_exclusions) = self
            .prepare_tun_dns_hijack(dns_hijack, auto_route, strict_route)
            .await?;
        let mut route_exclusions = if auto_route {
            dns_route_exclusions
        } else {
            Vec::new()
        };
        if auto_route {
            route_exclusions.retain(|address| tun_route_exclusion_required(*address));
            route_exclusions.sort_unstable();
            route_exclusions.dedup();
        }
        Ok(PreparedTunNetwork {
            dns_hijack,
            route_exclusions,
        })
    }

    #[cfg(any(test, not(feature = "raw-ip-runtime")))]
    async fn prepare_tun_dns_hijack(
        &self,
        requested: bool,
        auto_route: bool,
        strict: bool,
    ) -> Result<(bool, Vec<IpAddr>), EngineError> {
        let config = self.engine().config();
        let result = tokio::task::spawn_blocking(move || {
            config::prepare_dns_routes(
                config.as_ref(),
                requested,
                auto_route,
                zero_platform_tokio::system_dns_servers,
            )
        })
        .await
        .map_err(|error| io::Error::other(format!("system DNS discovery task failed: {error}")))
        .and_then(|result| result);
        match result {
            Ok(plan) => Ok(plan),
            Err(error) if strict => Err(EngineError::Io(error)),
            Err(error) => {
                warn!(error = %error, "TUN DNS route protection unavailable; DNS hijack disabled");
                Ok((false, Vec::new()))
            }
        }
    }

    pub async fn stop_tun(&self) -> Result<(), EngineError> {
        self.stop_tun_internal(false).await
    }

    async fn stop_tun_internal(&self, allow_configured: bool) -> Result<(), EngineError> {
        let _operation = self.tun_operation_lock.lock().await;
        self.stop_tun_locked(allow_configured, false).await
    }
    async fn stop_tun_locked(
        &self,
        allow_configured: bool,
        preserve_budget: bool,
    ) -> Result<(), EngineError> {
        #[cfg(not(feature = "raw-ip-runtime"))]
        let _ = preserve_budget;
        #[cfg(feature = "raw-ip-runtime")]
        if !allow_configured && self.has_tun_packet_budget_recovery() {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "System TUN budget replacement still retains its original recovery",
            )
            .into());
        }
        if !allow_configured
            && self
                .tun_info
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|info| info.managed_config.is_some())
        {
            return Err(EngineError::Io(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "TUN is managed by `runtime.tun`; remove it with config.apply instead",
            )));
        }
        let (id, shutdown, completion) = self
            .tun_control
            .lock()
            .unwrap()
            .as_ref()
            .map(|control| {
                (
                    control.id,
                    control.shutdown.clone(),
                    control.completion.clone(),
                )
            })
            .ok_or_else(|| {
                EngineError::Io(io::Error::new(
                    io::ErrorKind::NotFound,
                    "TUN is not running",
                ))
            })?;
        shutdown.send_replace(true);
        #[cfg(feature = "raw-ip-runtime")]
        completion.request_source_stop();
        // Both notifications and the actual original JoinHandles remain in
        // this same published control if this waiter is cancelled or expires.
        let stopped = completion
            .wait_main(tokio::time::Instant::now() + Duration::from_secs(5))
            .await;
        let route_cleanup = completion
            .wait_routes(tokio::time::Instant::now() + Duration::from_secs(5))
            .await;
        let result = completion::join_results(stopped, route_cleanup);
        #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
        let result = completion::join_results(
            result,
            completion
                .wait_native(tokio::time::Instant::now() + Duration::from_secs(5))
                .await,
        );
        let result = result.map_err(EngineError::Io);
        if completion.resources_complete().await {
            let retired = {
                let mut control = self.tun_control.lock().unwrap();
                if control.as_ref().is_some_and(|control| control.id == id) {
                    #[cfg(feature = "raw-ip-runtime")]
                    let keep = preserve_budget
                        && control
                            .as_ref()
                            .is_some_and(|control| control.budget_recovery.is_some());
                    #[cfg(not(feature = "raw-ip-runtime"))]
                    let keep = false;
                    if !keep {
                        control.take()
                    } else {
                        None
                    }
                } else {
                    None
                }
            };
            drop(retired);
            clear_egress_after_route_cleanup(&self.egress_interface, true);
            clear_matching_tun_info(self, id);
        }
        match &result {
            Ok(()) => {
                self.tun_last_error.lock().unwrap().take();
            }
            Err(error) => {
                *self.tun_last_error.lock().unwrap() = Some(error.to_string());
            }
        }
        result
    }
}

fn next_ip(address: IpAddr) -> Option<IpAddr> {
    match address {
        IpAddr::V4(address) => address
            .to_bits()
            .checked_add(1)
            .map(std::net::Ipv4Addr::from_bits)
            .map(IpAddr::V4),
        IpAddr::V6(address) => address
            .to_bits()
            .checked_add(1)
            .map(std::net::Ipv6Addr::from_bits)
            .map(IpAddr::V6),
    }
}

fn clear_egress_after_route_cleanup(
    control: &zero_platform_tokio::EgressInterfaceControl,
    cleanup_complete: bool,
) {
    if cleanup_complete {
        clear_egress_control(control, "tun_stopped");
    }
}

fn clear_egress_control(
    control: &zero_platform_tokio::EgressInterfaceControl,
    cause: &'static str,
) {
    let previous_generation = control.generation();
    control.clear();
    log_tun_generation_change(control, previous_generation, cause, None);
}

fn log_tun_generation_change(
    control: &zero_platform_tokio::EgressInterfaceControl,
    previous_generation: u64,
    cause: &'static str,
    reconciliation_changed: Option<bool>,
) {
    let generation = control.generation();
    if generation != previous_generation {
        info!(
            event_type = "tun_network_generation_changed",
            previous_generation,
            generation,
            cause,
            reconciliation_changed,
            "TUN network generation changed"
        );
    }
}

#[cfg(test)]
async fn finalize_tun_runtime_exit(proxy: &Proxy, id: u64) {
    clear_matching_tun_info(proxy, id);
    let retained = proxy
        .tun_control
        .lock()
        .unwrap()
        .as_ref()
        .filter(|control| control.id == id)
        .map(|control| (control.shutdown.clone(), control.completion.clone()));
    let Some((shutdown, completion)) = retained else {
        return;
    };
    shutdown.send_replace(true);
    // Main body exit cannot consume its own JoinHandle. Preserve its original
    // control until the actual stop observer joins it; routes have their own
    // independent original handle in the same retained completion.
    let route_cleanup = completion
        .wait_routes(tokio::time::Instant::now() + Duration::from_secs(5))
        .await;
    match route_cleanup {
        Ok(()) => clear_egress_control(&proxy.egress_interface, "tun_runtime_exited"),
        Err(error) => {
            warn!(%error, "retaining TUN physical egress after route cleanup failure");
            let mut last_error = proxy.tun_last_error.lock().unwrap();
            let error = format!("TUN route cleanup failed after runtime exit: {error}");
            *last_error = Some(match last_error.take() {
                Some(runtime_error) => format!("{runtime_error}; {error}"),
                None => error,
            });
        }
    }
}

fn clear_matching_tun_info(proxy: &Proxy, id: u64) {
    let mut info = proxy.tun_info.lock().unwrap();
    if info.as_ref().is_some_and(|info| info.id == id) {
        debug!(id, "clearing TUN runtime state");
        info.take();
    }
}

#[cfg(test)]
mod tests;

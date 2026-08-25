//! TUN inbound lifecycle and proxy-kernel integration.

mod config;
mod routes;
mod runtime;
mod sniff;
#[cfg(feature = "udp-runtime")]
mod udp;

use std::io;
use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tracing::{debug, info, warn};
use zero_engine::EngineError;
use zero_stack::UserNetworkStack;
use zero_tun::TunDevice;

use crate::runtime::{Proxy, TunControl, TunInfo};
use config::{configured_dns_endpoint_addresses, parse_interface_addresses};

static NEXT_TUN_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug)]
pub struct TunRuntimeOptions {
    pub auto_route: bool,
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
            dual_stack,
            strict_route,
            dns_hijack,
        } = options;
        let _operation = self.tun_operation_lock.lock().await;
        if !cfg!(feature = "udp-runtime") {
            return Err(EngineError::Io(io::Error::new(
                io::ErrorKind::Unsupported,
                "TUN requires the zero-proxy `udp-runtime` feature",
            )));
        }
        if self.tun_control.lock().unwrap().is_some() {
            return Err(EngineError::Io(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "TUN is already running",
            )));
        }
        if mtu < 576 {
            return Err(EngineError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "TUN MTU must be at least 576",
            )));
        }
        let interface_addresses =
            parse_interface_addresses(addr, mask, secondary_addr, dual_stack)?;
        let mut tun_owned_addresses = interface_addresses
            .iter()
            .map(|address| address.address)
            .collect::<Vec<_>>();
        tun_owned_addresses.extend(
            interface_addresses
                .iter()
                .filter_map(|address| next_ip(address.address)),
        );
        if let Some(conflict) = self.resolver.fake_ip_conflict(&tun_owned_addresses) {
            return Err(EngineError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "Fake-IP pool overlaps TUN-owned address `{conflict}`; choose a non-overlapping DNS fake-IP CIDR"
                ),
            )));
        }
        let primary = &interface_addresses[0];
        let address = primary.address;
        let prepared_network = match prepared_network {
            Some(prepared) => prepared,
            None => {
                self.prepare_tun_network(auto_route, dns_hijack, strict_route)
                    .await?
            }
        };
        let dns_hijack = prepared_network.dns_hijack;

        let device = zero_tun::create(name).map_err(EngineError::Io)?;
        let address_pairs = interface_addresses
            .iter()
            .map(|address| (address.address, address.netmask))
            .collect::<Vec<_>>();
        device
            .configure_addresses(&address_pairs, mtu)
            .map_err(EngineError::Io)?;
        let device_name = device.name().to_owned();
        debug!(name = %device_name, "TUN device configured");
        let (device_writer, device_reader) = device.into_channels().map_err(EngineError::Io)?;
        let network_responses = device_writer.clone();
        let stack = UserNetworkStack::new(device_writer, zero_stack::tcp_mss_for_mtu(mtu));
        let (tcp, udp) = stack.into_parts();

        let route_exclusions = prepared_network.route_exclusions;
        let route_addresses = interface_addresses
            .iter()
            .map(|address| (address.address, address.netmask))
            .collect::<Vec<_>>();
        let installed = if auto_route {
            routes::install(
                device_name.clone(),
                tag.to_owned(),
                route_addresses.clone(),
                route_exclusions.clone(),
                strict_route,
                self.egress_interface.clone(),
            )
            .await?
        } else {
            routes::InstalledRoutes {
                guards: Vec::new(),
                last_error: None,
            }
        };
        if auto_route {
            let missing_family = interface_addresses.iter().find(|address| {
                self.egress_interface
                    .current_for(address.address.is_ipv6())
                    .is_none()
            });
            if let Some(missing) = missing_family {
                let cleanup = routes::cleanup_guards(installed.guards).await;
                self.egress_interface.clear();
                cleanup.map_err(EngineError::Io)?;
                return Err(EngineError::Io(io::Error::new(
                    io::ErrorKind::NotConnected,
                    format!(
                        "TUN physical egress was not published for {} before route activation",
                        if missing.address.is_ipv6() {
                            "IPv6"
                        } else {
                            "IPv4"
                        }
                    ),
                )));
            }
        }
        let mut route_error = installed.last_error;
        self.egress_interface
            .replace_tunnel_addresses(interface_addresses.iter().map(|address| address.address));
        let route_monitor = if auto_route {
            match zero_tun::RouteChangeMonitor::new() {
                Ok(monitor) => Some(monitor),
                Err(error) if strict_route => {
                    let cleanup = routes::cleanup_guards(installed.guards).await;
                    self.egress_interface.clear();
                    cleanup.map_err(EngineError::Io)?;
                    return Err(EngineError::Io(error));
                }
                Err(error) => {
                    warn!(error = %error, "TUN route monitor unavailable; retrying in background");
                    route_error = Some(error.to_string());
                    None
                }
            }
        } else {
            None
        };
        let (egress_interface_v4, egress_interface_v6) = routes::route_names(&installed.guards);
        debug!("TUN physical egress bindings selected");
        let egress_interface = if address.is_ipv6() {
            egress_interface_v6.clone()
        } else {
            egress_interface_v4.clone()
        };
        let managed_by_config = managed_config.is_some();

        let id = NEXT_TUN_ID.fetch_add(1, Ordering::Relaxed);
        let (shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
        let (done_tx, done) = tokio::sync::oneshot::channel();
        let route_done = auto_route.then(|| {
            routes::spawn(
                self.clone(),
                routes::RouteRuntimeSpec {
                    id,
                    tun_name: device_name.clone(),
                    recovery_key: tag.to_owned(),
                    primary_ipv6: address.is_ipv6(),
                    addresses: route_addresses,
                    dns_hijack,
                },
                installed.guards,
                route_monitor,
                shutdown.subscribe(),
            )
        });
        *self.tun_info.lock().unwrap() = Some(TunInfo {
            id,
            name: device_name.clone(),
            addr: addr.to_owned(),
            addresses: interface_addresses
                .iter()
                .map(|address| address.cidr.clone())
                .collect(),
            mtu,
            tag: tag.to_owned(),
            auto_route,
            dual_stack,
            strict_route,
            dns_hijack,
            healthy: route_error.is_none(),
            last_error: route_error,
            egress_interface,
            egress_interface_v4,
            egress_interface_v6,
            route_exclusions,
            managed_config,
        });
        self.tun_last_error.lock().unwrap().take();
        *self.tun_control.lock().unwrap() = Some(TunControl {
            id,
            shutdown,
            done,
            route_done,
        });
        debug!("TUN runtime state published");

        info!(inbound_tag = tag, name = %device_name, %addr, mtu, "TUN device started");
        let proxy = self.clone();
        let inbound_tag = tag.to_owned();
        tokio::spawn(async move {
            let result = runtime::run(
                proxy.clone(),
                device_reader,
                tcp,
                udp,
                runtime::TunIngressConfig {
                    addresses: address_pairs,
                    tag: inbound_tag,
                    dns_hijack,
                    mtu: usize::from(mtu),
                    network_responses,
                },
                shutdown_rx,
            )
            .await;
            if let Err(error) = result {
                warn!(error = %error, "TUN runtime stopped unexpectedly");
                *proxy.tun_last_error.lock().unwrap() = Some(error.to_string());
                if managed_by_config {
                    let _ = proxy.configured_tun_failures.send(error.to_string());
                }
            }
            clear_matching_tun_state(&proxy, id);
            let _ = done_tx.send(());
        });

        Ok(())
    }

    pub(crate) async fn reconcile_configured_tun(
        &self,
        desired: Option<&zero_config::TunConfig>,
        network_mtu: u16,
    ) -> Result<(), EngineError> {
        let current = self.tun_info.lock().unwrap().clone();
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
        self.start_tun_internal(TunStartSpec {
            name: desired.name.as_deref(),
            addr: &desired.addr,
            mask: &desired.mask,
            secondary_addr: desired.secondary_addr.as_deref(),
            mtu: desired.effective_mtu(network_mtu),
            tag: &desired.tag,
            options: TunRuntimeOptions {
                auto_route: desired.auto_route,
                dual_stack: desired.dual_stack,
                strict_route: desired.strict_route,
                dns_hijack: desired.dns_hijack,
            },
            managed_config: Some(desired.clone()),
            prepared_network,
        })
        .await
    }

    pub(crate) async fn stop_tun_if_running(&self) -> Result<(), EngineError> {
        if self.tun_control.lock().unwrap().is_none() {
            return Ok(());
        }
        self.stop_tun_internal(true).await
    }

    async fn prepare_tun_network(
        &self,
        auto_route: bool,
        dns_hijack: bool,
        strict_route: bool,
    ) -> Result<PreparedTunNetwork, EngineError> {
        let (dns_hijack, dns_route_exclusions) =
            self.prepare_tun_dns_hijack(dns_hijack, strict_route)?;
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

    fn prepare_tun_dns_hijack(
        &self,
        requested: bool,
        strict: bool,
    ) -> Result<(bool, Vec<IpAddr>), EngineError> {
        if !requested {
            return Ok((false, Vec::new()));
        }
        let result = configured_dns_endpoint_addresses(self.engine().config().as_ref());
        match result {
            Ok(addresses) => Ok((true, addresses)),
            Err(error) if strict => Err(EngineError::Io(error)),
            Err(error) => {
                warn!(error = %error, "TUN DNS hijack disabled");
                Ok((false, Vec::new()))
            }
        }
    }

    pub async fn stop_tun(&self) -> Result<(), EngineError> {
        self.stop_tun_internal(false).await
    }

    async fn stop_tun_internal(&self, allow_configured: bool) -> Result<(), EngineError> {
        let _operation = self.tun_operation_lock.lock().await;
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
        let control = self.tun_control.lock().unwrap().take().ok_or_else(|| {
            EngineError::Io(io::Error::new(
                io::ErrorKind::NotFound,
                "TUN is not running",
            ))
        })?;
        let TunControl {
            id,
            shutdown,
            done,
            route_done,
        } = control;
        let _ = shutdown.send(true);
        let stopped = tokio::time::timeout(Duration::from_secs(5), done).await;
        let route_cleanup = match route_done {
            Some(done) => match tokio::time::timeout(Duration::from_secs(5), done).await {
                Ok(Ok(Ok(()))) => Ok(()),
                Ok(Ok(Err(error))) => Err(EngineError::Io(io::Error::other(error))),
                Ok(Err(_)) => Err(EngineError::Io(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "TUN route runtime exited without shutdown acknowledgement",
                ))),
                Err(_) => Err(EngineError::Io(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "timed out waiting for TUN route runtime shutdown",
                ))),
            },
            None => Ok(()),
        };
        self.egress_interface.clear();
        clear_matching_tun_info(self, id);
        let result = match stopped {
            Ok(Ok(())) => route_cleanup,
            Ok(Err(_)) => Err(EngineError::Io(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "TUN runtime exited without shutdown acknowledgement",
            ))),
            Err(_) => Err(EngineError::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                "timed out waiting for TUN runtime shutdown",
            ))),
        };
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

fn clear_matching_tun_state(proxy: &Proxy, id: u64) {
    clear_matching_tun_info(proxy, id);
    let removed = {
        let mut control = proxy.tun_control.lock().unwrap();
        if control.as_ref().is_some_and(|control| control.id == id) {
            control.take()
        } else {
            None
        }
    };
    if removed.is_some() {
        drop(removed);
        proxy.egress_interface.clear();
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

use std::io;
use std::net::IpAddr;
use std::time::Duration;

use ipnet::IpNet;
use tokio::sync::{oneshot, watch};
use zero_engine::EngineError;
use zero_tun::{
    capture_route_prefixes_with_exclusions, strict_route_socket_mark, RouteChangeMonitor,
    RouteInterface, SystemLeakGuard, SystemRouteGuard,
};

use crate::runtime::Proxy;

mod state;
use state::{publish_error, publish_state, publish_unavailable};

const ROUTE_EVENT_DEBOUNCE: Duration = Duration::from_millis(400);
const ROUTE_WATCHDOG_INTERVAL: Duration = Duration::from_secs(30);
const ROUTE_RETRY_DELAYS: [Duration; 4] = [
    Duration::from_millis(250),
    Duration::from_secs(1),
    Duration::from_secs(3),
    Duration::from_secs(10),
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReconcileTrigger {
    PlatformEvent,
    Retry,
    Watchdog,
}

impl ReconcileTrigger {
    fn should_debounce(self) -> bool {
        matches!(self, Self::PlatformEvent)
    }
}

pub(super) struct InstalledRoutes {
    pub guards: Vec<SystemRouteGuard>,
    pub leak_guard: Option<SystemLeakGuard>,
    pub last_error: Option<String>,
}

pub(super) struct RouteInstallSpec {
    pub tun_name: String,
    pub recovery_key: String,
    pub addresses: Vec<(IpAddr, IpAddr)>,
    pub include_cidrs: Vec<IpNet>,
    pub exclude_cidrs: Vec<IpNet>,
    pub excluded: Vec<IpAddr>,
    pub strict: bool,
}

pub(super) async fn install(
    spec: RouteInstallSpec,
    egress_control: zero_platform_tokio::EgressInterfaceControl,
) -> Result<InstalledRoutes, EngineError> {
    let RouteInstallSpec {
        tun_name,
        recovery_key,
        addresses,
        include_cidrs,
        exclude_cidrs,
        excluded,
        strict,
    } = spec;
    tokio::task::spawn_blocking(move || {
        let socket_mark = strict.then(|| strict_route_socket_mark(&recovery_key));
        let mut guards = Vec::new();
        let mut last_error = None;
        let previous_v4 = egress_control.current_for(false);
        let previous_v6 = egress_control.current_for(true);
        let protected = addresses
            .iter()
            .flat_map(|(address, _)| {
                capture_route_prefixes_with_exclusions(*address, &include_cidrs, &exclude_cidrs)
            })
            .collect::<Vec<_>>();
        for (address, netmask) in addresses {
            let ipv6 = address.is_ipv6();
            let previous = if ipv6 {
                previous_v6.clone()
            } else {
                previous_v4.clone()
            };
            let published_egress = egress_control.clone();
            match SystemRouteGuard::install_with_egress(
                &tun_name,
                &recovery_key,
                address,
                netmask,
                &capture_route_prefixes_with_exclusions(address, &include_cidrs, &exclude_cidrs),
                &excluded,
                move |route| {
                    let interface = platform_egress_interface(route, socket_mark)?;
                    published_egress.replace_for(ipv6, Some(interface));
                    Ok(())
                },
            ) {
                Ok(guard) => {
                    let interface = platform_egress_interface(guard.egress(), socket_mark)?;
                    egress_control.replace_for(ipv6, Some(interface));
                    guards.push(guard);
                }
                Err(error) if strict => {
                    let mut rollback_error = None;
                    for guard in guards.drain(..).rev() {
                        if let Err(cleanup) = guard.close() {
                            rollback_error.get_or_insert(cleanup);
                        }
                    }
                    egress_control.replace_for(false, previous_v4);
                    egress_control.replace_for(true, previous_v6);
                    let error = match rollback_error {
                        Some(rollback) => io::Error::new(
                            error.kind(),
                            format!("{error}; rollback automatic TUN routes: {rollback}"),
                        ),
                        None => error,
                    };
                    return Err(EngineError::Io(error));
                }
                Err(error) => {
                    egress_control.replace_for(ipv6, previous);
                    tracing::warn!(
                        family = if address.is_ipv6() { "IPv6" } else { "IPv4" },
                        error = %error,
                        "automatic TUN route installation skipped"
                    );
                    last_error = Some(error.to_string());
                }
            }
        }
        let leak_guard = if strict {
            match SystemLeakGuard::install(&tun_name, &recovery_key, &protected, &excluded) {
                Ok(guard) => Some(guard),
                Err(error) => {
                    let mut rollback_error = None;
                    for guard in guards.drain(..).rev() {
                        if let Err(cleanup) = guard.close() {
                            rollback_error.get_or_insert(cleanup);
                        }
                    }
                    egress_control.replace_for(false, previous_v4);
                    egress_control.replace_for(true, previous_v6);
                    let error = match rollback_error {
                        Some(rollback) => io::Error::new(
                            error.kind(),
                            format!("{error}; rollback automatic TUN routes: {rollback}"),
                        ),
                        None => error,
                    };
                    return Err(EngineError::Io(error));
                }
            }
        } else {
            None
        };
        Ok(InstalledRoutes {
            guards,
            leak_guard,
            last_error,
        })
    })
    .await
    .map_err(|error| {
        EngineError::Io(io::Error::other(format!(
            "TUN route task panicked: {error}"
        )))
    })?
}

pub(super) fn platform_egress_interface(
    route: &RouteInterface,
    socket_mark: Option<u32>,
) -> io::Result<zero_platform_tokio::EgressInterface> {
    let interface =
        zero_platform_tokio::EgressInterface::new(route.name().to_owned(), route.index())?;
    match socket_mark {
        Some(mark) => interface.with_socket_mark(mark),
        None => Ok(interface),
    }
}

pub(super) struct RouteRuntimeSpec {
    pub id: u64,
    pub tun_name: String,
    pub recovery_key: String,
    pub primary_ipv6: bool,
    pub addresses: Vec<(IpAddr, IpAddr)>,
    pub include_cidrs: Vec<IpNet>,
    pub exclude_cidrs: Vec<IpNet>,
    pub dns_hijack: bool,
    pub strict_route: bool,
}

impl RouteRuntimeSpec {
    fn managed_families(&self) -> (bool, bool) {
        let managed_v4 = self.addresses.iter().any(|(address, _)| address.is_ipv4());
        let managed_v6 = self.addresses.iter().any(|(address, _)| address.is_ipv6());
        (managed_v4, managed_v6)
    }
}

pub(super) fn spawn(
    proxy: Proxy,
    spec: RouteRuntimeSpec,
    guards: Vec<SystemRouteGuard>,
    leak_guard: Option<SystemLeakGuard>,
    monitor: Option<RouteChangeMonitor>,
    shutdown: watch::Receiver<bool>,
) -> oneshot::Receiver<Result<(), String>> {
    let (done_tx, done) = oneshot::channel();
    tokio::spawn(async move {
        let result = run(proxy, spec, guards, leak_guard, monitor, shutdown).await;
        let _ = done_tx.send(result.map_err(|error| error.to_string()));
    });
    done
}

async fn run(
    proxy: Proxy,
    spec: RouteRuntimeSpec,
    mut guards: Vec<SystemRouteGuard>,
    mut leak_guard: Option<SystemLeakGuard>,
    mut monitor: Option<RouteChangeMonitor>,
    mut shutdown: watch::Receiver<bool>,
) -> io::Result<()> {
    // Re-read once after notification registration to close the small race
    // between initial route installation and monitor creation.
    let mut retry_index = Some(0_usize);
    loop {
        let trigger = if let Some(index) = retry_index {
            let delay = ROUTE_RETRY_DELAYS[index.min(ROUTE_RETRY_DELAYS.len() - 1)];
            tokio::select! {
                changed = shutdown.changed() => {
                    let _ = changed;
                    break;
                }
                changed = monitor_changed(&mut monitor) => {
                    if let Err(error) = changed {
                        monitor = None;
                        publish_runtime_error(&proxy, &spec, error.to_string());
                    }
                    ReconcileTrigger::PlatformEvent
                }
                _ = tokio::time::sleep(delay) => ReconcileTrigger::Retry,
            }
        } else {
            tokio::select! {
                changed = shutdown.changed() => {
                    let _ = changed;
                    break;
                }
                changed = monitor_changed(&mut monitor) => {
                    if let Err(error) = changed {
                        monitor = None;
                        publish_runtime_error(&proxy, &spec, error.to_string());
                        retry_index = Some(0);
                    }
                    ReconcileTrigger::PlatformEvent
                }
                _ = tokio::time::sleep(ROUTE_WATCHDOG_INTERVAL) => ReconcileTrigger::Watchdog,
            }
        };
        if *shutdown.borrow() {
            break;
        }

        if trigger.should_debounce() {
            tokio::select! {
                changed = shutdown.changed() => {
                    let _ = changed;
                    break;
                }
                _ = tokio::time::sleep(ROUTE_EVENT_DEBOUNCE) => {}
            }
            if let Some(active_monitor) = monitor.as_mut() {
                if let Err(error) = active_monitor.coalesce() {
                    publish_runtime_error(&proxy, &spec, error.to_string());
                    monitor = None;
                }
            }
        }
        if trigger == ReconcileTrigger::Watchdog {
            tracing::debug!(
                tun = %spec.tun_name,
                "auditing TUN routes after the network lifecycle watchdog interval"
            );
        }
        if monitor.is_none() {
            match RouteChangeMonitor::new() {
                Ok(new_monitor) => monitor = Some(new_monitor),
                Err(error) => {
                    publish_runtime_error(&proxy, &spec, error.to_string());
                    retry_index = Some(next_retry(retry_index));
                    continue;
                }
            }
        }

        let prepared = tokio::select! {
            changed = shutdown.changed() => {
                let _ = changed;
                break;
            }
            prepared = proxy.prepare_tun_network(
                true,
                spec.dns_hijack,
                // Reconciliation requires a complete explicit bypass
                // snapshot. A transient DNS bootstrap failure retains the
                // last usable routes even when startup used non-strict mode.
                true,
            ) => prepared,
        };
        let exclusions = match prepared {
            Ok(prepared) => prepared.route_exclusions,
            Err(error) => {
                publish_runtime_error(&proxy, &spec, error.to_string());
                retry_index = Some(next_retry(retry_index));
                continue;
            }
        };

        let tun_name = spec.tun_name.clone();
        let recovery_key = spec.recovery_key.clone();
        let addresses = spec.addresses.clone();
        let include_cidrs = spec.include_cidrs.clone();
        let exclude_cidrs = spec.exclude_cidrs.clone();
        let protected = addresses
            .iter()
            .flat_map(|(address, _)| {
                capture_route_prefixes_with_exclusions(*address, &include_cidrs, &exclude_cidrs)
            })
            .collect::<Vec<_>>();
        let reconcile_exclusions = exclusions.clone();
        let reconciled = tokio::task::spawn_blocking(move || {
            let result = reconcile_guards(
                &mut guards,
                &tun_name,
                &recovery_key,
                &addresses,
                &include_cidrs,
                &exclude_cidrs,
                &reconcile_exclusions,
            )
            .and_then(|changed| {
                let leak_changed = leak_guard
                    .as_mut()
                    .map(|guard| guard.reconcile(&protected, &reconcile_exclusions))
                    .transpose()?
                    .unwrap_or(false);
                Ok(changed || leak_changed)
            });
            (guards, leak_guard, result)
        })
        .await;
        let (returned_guards, returned_leak_guard, result) = match reconciled {
            Ok(result) => result,
            Err(error) => {
                publish_runtime_error(&proxy, &spec, format!("TUN route task panicked: {error}"));
                return Err(io::Error::other(format!(
                    "TUN route task panicked: {error}"
                )));
            }
        };
        guards = returned_guards;
        leak_guard = returned_leak_guard;
        match result {
            Ok(changed) => {
                if let Err(error) = publish_state(&proxy, &spec, &guards, Some(exclusions), None) {
                    publish_runtime_error(&proxy, &spec, error.to_string());
                    return Err(error);
                }
                if changed {
                    tracing::info!(tun = %spec.tun_name, "TUN physical egress routes reconciled");
                }
                retry_index = None;
            }
            Err(error) => {
                tracing::warn!(
                    tun = %spec.tun_name,
                    error = %error,
                    fail_closed = spec.strict_route,
                    "TUN route reconciliation failed"
                );
                let message = error.to_string();
                if spec.strict_route {
                    publish_unavailable(&proxy, &spec, message);
                } else if let Err(status_error) =
                    publish_state(&proxy, &spec, &guards, None, Some(message.clone()))
                {
                    publish_error(
                        &proxy,
                        spec.id,
                        format!("{message}; publish route state: {status_error}"),
                    );
                }
                retry_index = Some(next_retry(retry_index));
            }
        }
    }

    cleanup_guards(guards, leak_guard).await
}

fn publish_runtime_error(proxy: &Proxy, spec: &RouteRuntimeSpec, error: String) {
    if spec.strict_route {
        publish_unavailable(proxy, spec, error);
    } else {
        publish_error(proxy, spec.id, error);
    }
}

async fn monitor_changed(monitor: &mut Option<RouteChangeMonitor>) -> io::Result<()> {
    match monitor.as_mut() {
        Some(monitor) => monitor.changed().await,
        None => std::future::pending().await,
    }
}

fn next_retry(current: Option<usize>) -> usize {
    current
        .map(|index| index.saturating_add(1))
        .unwrap_or(0)
        .min(ROUTE_RETRY_DELAYS.len() - 1)
}

fn reconcile_guards(
    guards: &mut Vec<SystemRouteGuard>,
    tun_name: &str,
    recovery_key: &str,
    addresses: &[(IpAddr, IpAddr)],
    include_cidrs: &[IpNet],
    exclude_cidrs: &[IpNet],
    excluded: &[IpAddr],
) -> io::Result<bool> {
    let mut changed = false;
    for (address, netmask) in addresses.iter().copied() {
        if let Some(guard) = guards
            .iter_mut()
            .find(|guard| guard.is_ipv6() == address.is_ipv6())
        {
            changed |= guard.reconcile(excluded)?;
        } else {
            guards.push(SystemRouteGuard::install(
                tun_name,
                recovery_key,
                address,
                netmask,
                &capture_route_prefixes_with_exclusions(address, include_cidrs, exclude_cidrs),
                excluded,
            )?);
            changed = true;
        }
    }
    Ok(changed)
}

pub(super) async fn cleanup_guards(
    guards: Vec<SystemRouteGuard>,
    leak_guard: Option<SystemLeakGuard>,
) -> io::Result<()> {
    tokio::task::spawn_blocking(move || {
        let mut result = Ok(());
        if let Some(guard) = leak_guard {
            if let Err(error) = guard.close() {
                result = Err(error);
            }
        }
        for guard in guards.into_iter().rev() {
            if let Err(error) = guard.close() {
                if result.is_ok() {
                    result = Err(error);
                }
            }
        }
        result
    })
    .await
    .map_err(|error| io::Error::other(format!("TUN route cleanup task panicked: {error}")))?
}

pub(super) fn route_names(guards: &[SystemRouteGuard]) -> (Option<String>, Option<String>) {
    state::route_names(guards)
}

#[cfg(test)]
mod tests {
    use super::{
        next_retry, ReconcileTrigger, RouteRuntimeSpec, ROUTE_RETRY_DELAYS, ROUTE_WATCHDOG_INTERVAL,
    };

    #[test]
    fn route_retry_backoff_starts_small_and_is_bounded() {
        assert_eq!(next_retry(None), 0);
        assert_eq!(next_retry(Some(0)), 1);
        assert_eq!(
            next_retry(Some(ROUTE_RETRY_DELAYS.len() - 1)),
            ROUTE_RETRY_DELAYS.len() - 1
        );
    }

    #[test]
    fn only_platform_events_are_debounced() {
        assert!(ReconcileTrigger::PlatformEvent.should_debounce());
        assert!(!ReconcileTrigger::Retry.should_debounce());
        assert!(!ReconcileTrigger::Watchdog.should_debounce());
        assert!(ROUTE_WATCHDOG_INTERVAL > ROUTE_RETRY_DELAYS[0]);
    }

    #[test]
    fn runtime_spec_tracks_only_managed_address_families() {
        let ipv4 = RouteRuntimeSpec {
            id: 1,
            tun_name: "tun0".to_owned(),
            recovery_key: "tun-in".to_owned(),
            primary_ipv6: false,
            addresses: vec![(
                "10.66.0.1".parse().unwrap(),
                "255.255.255.0".parse().unwrap(),
            )],
            include_cidrs: Vec::new(),
            exclude_cidrs: Vec::new(),
            dns_hijack: true,
            strict_route: true,
        };
        assert_eq!(ipv4.managed_families(), (true, false));

        let dual_stack = RouteRuntimeSpec {
            addresses: vec![
                ipv4.addresses[0],
                (
                    "fd66::1".parse().unwrap(),
                    "ffff:ffff:ffff:ffff::".parse().unwrap(),
                ),
            ],
            ..ipv4
        };
        assert_eq!(dual_stack.managed_families(), (true, true));
    }
}

//! This is the original blocking SDK worker, not a wrapper joining another task.
use super::*;
pub(super) fn run(original: Arc<Shared>) -> Result<(), ChannelError> {
    let mut received = original
        .received
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .take()
        .ok_or_else(|| {
            error(
                Reason::StalePlan,
                "same route worker input was already consumed",
            )
        })?;
    let mut fault = None;
    while !original.closing.load(Ordering::Acquire) {
        let Some(job) = received.blocking_recv() else {
            break;
        };
        match job {
            Job::Discover(config, completed) => {
                let (hijack, strict) = {
                    let native = original
                        .native
                        .lock()
                        .unwrap_or_else(|error| error.into_inner());
                    (native.spec.dns_hijack, native.spec.strict_route)
                };
                let result = prepare(&config, hijack).or_else(|source| {
                    if strict { Err(source) } else {
                        tracing::warn!(%source,"TUN DNS interception disabled after preparation failure");
                        Ok(crate::inbound::tun::PreparedTunNetwork {dns_hijack:false,route_exclusions:vec![]})
                    }
                });
                if let Err(source) = &result {
                    fault.get_or_insert_with(|| source.clone());
                }
                original
                    .observed
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .prepared = Some(result.clone());
                if let Some(completed) = completed {
                    let _ = completed.send(result);
                }
                original.changed.notify_waiters();
            }
            Job::Install(name, completed) => {
                let prepared = original
                    .observed
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .prepared
                    .clone();
                let result = match prepared {
                    Some(Ok(prepared)) => original
                        .native
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .install(
                            name,
                            &prepared.route_exclusions,
                            &original.proxy.egress_interface,
                        )
                        .map_err(Arc::new),
                    Some(Err(source)) => Err(source),
                    None => Err(Arc::new(io::Error::other(
                        "native route install preceded actual DNS preparation",
                    ))),
                };
                if let Err(source) = &result {
                    fault.get_or_insert_with(|| source.clone());
                }
                let _ = completed.send(result);
            }
            Job::Reconcile(config, completed) => {
                let hijack = original
                    .native
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .spec
                    .dns_hijack;
                let result = prepare(&config, hijack).and_then(|prepared| {
                    original
                        .native
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .reconcile(&prepared.route_exclusions, &original.proxy.egress_interface)
                        .map_err(Arc::new)
                });
                // Transient maintenance errors remain visible to this actual
                // recovery request. The worker keeps the same native originals.
                let _ = completed.send(result);
            }
            Job::Close => break,
        }
    }
    received.close();
    // This waits for a different real registration, never our own body or join.
    // The external owning Source Scope consumes its actual native exit.
    if let Some(monitor) = original.monitor.get() {
        let _ = tokio::runtime::Handle::current().block_on(monitor.wait());
    }
    let mut close_fault = None;
    loop {
        let monitor = original.monitor_io.blocking_lock().close();
        let routes = original
            .native
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .close();
        let failures = monitor
            .err()
            .into_iter()
            .chain(routes.err())
            .collect::<Vec<_>>();
        match if failures.is_empty() {
            Ok(())
        } else {
            Err(io::Error::other(CleanupFailures(failures)))
        } {
            Ok(()) => break,
            Err(source) => {
                close_fault.get_or_insert_with(|| Arc::new(source));
                // Failed cleanup keeps this actual worker and native originals
                // alive. Shutdown deadlines report unfinished without aborting
                // its SDK command or replacing the controller.
                std::thread::park_timeout(ROUTE_RETRY_DELAYS[0]);
            }
        }
    }
    original
        .observed
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .physically_closed = true;
    original.changed.notify_waiters();
    let failures = fault
        .into_iter()
        .chain(close_fault)
        .map(|source| io::Error::other(SharedError(source)))
        .collect::<Vec<_>>();
    if failures.is_empty() {
        Ok(())
    } else {
        Err(error(
            Reason::Io,
            "original route worker retained its actual failures",
        )
        .with_source(CleanupFailures(failures)))
    }
}
fn prepare(
    config: &zero_config::RuntimeConfig,
    hijack: bool,
) -> Result<crate::inbound::tun::PreparedTunNetwork, SourceError> {
    let (dns_hijack, mut route_exclusions) = crate::inbound::tun::config::prepare_dns_routes(
        config,
        hijack,
        true,
        zero_platform_tokio::system_dns_servers,
    )
    .map_err(Arc::new)?;
    route_exclusions.retain(|address| crate::inbound::tun::tun_route_exclusion_required(*address));
    route_exclusions.sort_unstable();
    route_exclusions.dedup();
    Ok(crate::inbound::tun::PreparedTunNetwork {
        dns_hijack,
        route_exclusions,
    })
}

#[derive(Debug)]
struct CleanupFailures(Vec<io::Error>);
impl std::fmt::Display for CleanupFailures {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (index, source) in self.0.iter().enumerate() {
            if index != 0 {
                f.write_str("; ")?;
            }
            source.fmt(f)?;
        }
        Ok(())
    }
}
impl std::error::Error for CleanupFailures {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.first().map(|source| source as _)
    }
}

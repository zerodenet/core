//! This is the original blocking SDK worker, not a wrapper joining another task.
use super::*;
mod cleanup;
pub(super) fn run(original: PacketMetadataArc<Shared>) -> Result<(), ChannelError> {
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
    let route_memory = original
        .proxy
        .protocols
        .runtime_host()
        .original_budgets()
        .packet
        .driver_metadata_view();
    #[cfg(target_os = "macos")]
    let mut helper: Option<zero_tun::MacosHelperLease> = None;
    #[cfg(target_os = "macos")]
    let mut helper_context: Option<zero_tun::MacosHelperContext> = None;
    // Clone only the already paid pure diagnostic payload. This cannot pin
    // native IO or create work, and outlives every concrete Native lock below.
    let diagnostics = original
        .native
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .diagnostics();
    let mut fault = None;
    let operation = unwind::attempt(|| -> Result<(), ChannelError> {
        #[cfg(target_os = "macos")]
        if original
            .native
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .requires_helper()
        {
            helper = Some(original.metadata.macos_helper_lease()?);
            helper_context = Some(helper.as_ref().unwrap().enter().map_err(|source| {
                error(Reason::Io, "same Source helper context failed").with_source(source)
            })?);
        }
        while !original.closing.load(Ordering::Acquire) {
            let Some(job) = received.blocking_recv() else {
                break;
            };
            match job {
                #[cfg(test)]
                Job::Panic => panic!("original route worker injected job panic"),
                Job::Discover(config, completed) => {
                    let (hijack, strict) = {
                        let native = original
                            .native
                            .lock()
                            .unwrap_or_else(|error| error.into_inner());
                        (native.spec.dns_hijack, native.spec.strict_route)
                    };
                    let result = prepare(&config, hijack, &original).or_else(|source| {
                    if strict { Err(source) } else {
                        tracing::warn!(%source,"TUN DNS interception disabled after preparation failure");
                        Ok(crate::inbound::tun::PreparedTunNetwork {dns_hijack:false,route_exclusions:vec![]})
                    }
                });
                    if let Err(source) = &result {
                        fault.get_or_insert_with(|| source.clone());
                        diagnostics.record_operation(source.clone());
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
                        Some(Ok(prepared)) => {
                            let notifications = route_memory.notifications();
                            let result = original
                                .native
                                .lock()
                                .unwrap_or_else(|error| error.into_inner())
                                .install(
                                    name,
                                    &prepared.route_exclusions,
                                    &original.proxy.egress_interface,
                                )
                                .map_err(Arc::new);
                            drop(notifications);
                            result
                        }
                        Some(Err(source)) => Err(source),
                        None => Err(Arc::new(io::Error::other(
                            "native route install preceded actual DNS preparation",
                        ))),
                    };
                    if let Err(source) = &result {
                        fault.get_or_insert_with(|| source.clone());
                        diagnostics.record_operation(source.clone());
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
                    let result = prepare(&config, hijack, &original).and_then(|prepared| {
                        let notifications = route_memory.notifications();
                        let result = original
                            .native
                            .lock()
                            .unwrap_or_else(|error| error.into_inner())
                            .reconcile(&prepared.route_exclusions, &original.proxy.egress_interface)
                            .map_err(Arc::new);
                        drop(notifications);
                        result
                    });
                    // Transient maintenance errors remain visible to this actual
                    // recovery request. The worker keeps the same native originals.
                    let _ = completed.send(result);
                }
                Job::Close => break,
            }
        }
        Ok(())
    });
    let mut operation_panic = None;
    match operation {
        Ok(Ok(())) => {}
        Ok(Err(source)) => {
            let source = Arc::new(io::Error::other(source));
            diagnostics.record_operation(source.clone());
            fault.get_or_insert(source);
        }
        Err(panic) => {
            let source = panic.source();
            diagnostics.record_operation(source.clone());
            fault.get_or_insert(source);
            operation_panic = Some(panic);
        }
    }
    // This same original stops new jobs and its monitor before native cleanup;
    // neither the caller's cancellation nor a failed job can discard custody.
    original.closing.store(true, Ordering::Release);
    original.cancel.cancel();
    received.close();
    let (close_fault, cleanup_panic) = cleanup::run(
        &original,
        &route_memory,
        &diagnostics,
        #[cfg(target_os = "macos")]
        &mut helper,
        #[cfg(target_os = "macos")]
        &mut helper_context,
    );
    #[cfg(target_os = "macos")]
    {
        drop(helper_context.take());
        drop(helper); // actual route responsibility ends, waking original actor
    }
    original
        .observed
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .physically_closed = true;
    original.changed.notify_waiters();
    let result = diagnostics.finish_worker(fault, close_fault);
    if let Some(panic) = operation_panic.or(cleanup_panic) {
        panic.resume();
    }
    result
}
fn prepare(
    config: &zero_config::RuntimeConfig,
    hijack: bool,
    original: &Shared,
) -> Result<crate::inbound::tun::PreparedTunNetwork, SourceError> {
    let (dns_hijack, mut route_exclusions) =
        crate::inbound::tun::config::prepare_dns_routes(config, hijack, true, || {
            #[cfg(target_os = "macos")]
            {
                original.metadata.macos_system_dns_servers()
            }
            #[cfg(not(target_os = "macos"))]
            {
                let _ = original;
                zero_platform_tokio::system_dns_servers()
            }
        })
        .map_err(Arc::new)?;
    {
        let native = original
            .native
            .lock()
            .unwrap_or_else(|source| source.into_inner());
        for &(address, mask) in &native.spec.addresses {
            let local = zero_tun::tun_local_address_facts(address, mask).map_err(Arc::new)?;
            route_exclusions.retain(|peer| !local.contains(&Some(*peer)));
        }
    }
    route_exclusions.retain(|address| crate::inbound::tun::tun_route_exclusion_required(*address));
    route_exclusions.sort_unstable();
    route_exclusions.dedup();
    Ok(crate::inbound::tun::PreparedTunNetwork {
        dns_hijack,
        route_exclusions,
    })
}

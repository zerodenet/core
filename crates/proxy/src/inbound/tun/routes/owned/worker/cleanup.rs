//! Tail of the same original blocking worker; no replacement task or owner.
use super::*;
pub(super) fn run(
    original: &Shared,
    route_memory: &zero_tun::packet_memory::PacketDriverMetadataView,
    diagnostics: &super::super::diagnostics::RouteDiagnostics,
    #[cfg(target_os = "macos")] helper: &mut Option<zero_tun::MacosHelperLease>,
    #[cfg(target_os = "macos")] helper_context: &mut Option<zero_tun::MacosHelperContext>,
) -> (Option<SourceError>, Option<Arc<unwind::Panic>>) {
    let mut close_fault = None;
    let mut cleanup_panic = None;
    // Wait the actual other registration; this worker never joins itself.
    if let Some(monitor) = original.monitor.get() {
        loop {
            match unwind::attempt(|| tokio::runtime::Handle::current().block_on(monitor.wait())) {
                Ok(_original_join) => break,
                Err(panic) => {
                    let source = panic.source();
                    diagnostics.record_cleanup(source.clone());
                    close_fault.get_or_insert(source);
                    cleanup_panic.get_or_insert(panic);
                    std::thread::park_timeout(ROUTE_RETRY_DELAYS[0]);
                }
            }
        }
    }
    #[cfg(test)]
    if let Some((entered, resume)) = original.cleanup_gate.lock().unwrap().take() {
        let _ = entered.send(());
        let _ = resume.recv();
    }
    loop {
        // Keep these independent: a monitor failure/panic cannot suppress an
        // attempt to retire the same retained route/PF/File originals.
        let result = unwind::attempt(|| -> io::Result<bool> {
            #[cfg(target_os = "macos")]
            if let Some(helper) = helper.as_ref() {
                drop(helper_context.take());
                match helper.prepare_cleanup() {
                    Ok(context) => *helper_context = Some(context),
                    Err(source) if source.kind() == io::ErrorKind::WouldBlock => {
                        tokio::runtime::Handle::current()
                            .block_on(original.metadata.wait_macos_helper_actor_returned())
                            .map_err(io::Error::other)?;
                        return Ok(false);
                    }
                    Err(source) => return Err(source),
                }
            }
            Ok(true)
        });
        match result {
            Ok(Ok(true)) => {}
            Ok(Ok(false)) => continue,
            Ok(Err(source)) => {
                let source = Arc::new(source);
                diagnostics.record_cleanup(source.clone());
                close_fault.get_or_insert(source);
                std::thread::park_timeout(ROUTE_RETRY_DELAYS[0]);
                continue;
            }
            Err(panic) => {
                let source = panic.source();
                diagnostics.record_cleanup(source.clone());
                close_fault.get_or_insert(source);
                cleanup_panic.get_or_insert(panic);
                std::thread::park_timeout(ROUTE_RETRY_DELAYS[0]);
                continue;
            }
        }
        let monitor = unwind::attempt(|| original.monitor_io.blocking_lock().close());
        let routes = unwind::attempt(|| {
            let notifications = route_memory.notifications();
            let result = original
                .native
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .close();
            drop(notifications);
            result
        });
        let mut complete = true;
        for result in [monitor, routes] {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(source)) => {
                    complete = false;
                    let source = Arc::new(source);
                    diagnostics.record_cleanup(source.clone());
                    close_fault.get_or_insert(source);
                }
                Err(panic) => {
                    complete = false;
                    let source = panic.source();
                    diagnostics.record_cleanup(source.clone());
                    close_fault.get_or_insert(source);
                    cleanup_panic.get_or_insert(panic);
                }
            }
        }
        #[cfg(target_os = "macos")]
        if complete {
            if let Some(helper) = helper.as_ref() {
                // A dedicated helper's successful route epoch intentionally
                // leaves its actual actor's closure to that original Scope.
                // A reported failure can still have a positive close outcome;
                // preserve the fault without mistaking it for perpetual custody.
                match unwind::attempt(|| helper.finish_cleanup()) {
                    Ok(Ok(())) => {}
                    result => {
                        match result {
                            Ok(Err(source)) => {
                                let source = Arc::new(source);
                                diagnostics.record_cleanup(source.clone());
                                close_fault.get_or_insert(source);
                            }
                            Err(panic) => {
                                let source = panic.source();
                                diagnostics.record_cleanup(source.clone());
                                close_fault.get_or_insert(source);
                                cleanup_panic.get_or_insert(panic);
                            }
                            Ok(Ok(())) => unreachable!(),
                        }
                        match unwind::attempt(|| helper.require_closed()) {
                            Ok(Ok(())) => {}
                            Ok(Err(source)) => {
                                complete = false;
                                let source = Arc::new(source);
                                diagnostics.record_cleanup(source.clone());
                                close_fault.get_or_insert(source);
                            }
                            Err(panic) => {
                                complete = false;
                                let source = panic.source();
                                diagnostics.record_cleanup(source.clone());
                                close_fault.get_or_insert(source);
                                cleanup_panic.get_or_insert(panic);
                            }
                        }
                    }
                }
            }
        }
        if complete {
            break;
        }
        // A timeout observer never aborts this original or starts a successor.
        std::thread::park_timeout(ROUTE_RETRY_DELAYS[0]);
    }
    (close_fault, cleanup_panic)
}

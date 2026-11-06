//! The original route-monitor task drives the single retained SDK job lane.
use super::*;
pub(super) async fn run(
    original: Arc<Shared>,
    recovery: mpsc::Receiver<oneshot::Sender<Result<(), String>>>,
) -> Result<(), ChannelError> {
    let result = run_inner(&original, recovery).await;
    original.closing.store(true, Ordering::Release);
    let _ = original.jobs.try_send(Job::Close);
    result
}
async fn run_inner(
    original: &Arc<Shared>,
    mut recovery: mpsc::Receiver<oneshot::Sender<Result<(), String>>>,
) -> Result<(), ChannelError> {
    let interface = tokio::select! { biased;
        _ = original.cancel.cancelled()=>return Ok(()),
        result=original.metadata.wait_acquired(&original.cancel)=>match result {
            Ok(interface)=>interface,
            Err(_) if original.cancel.is_cancelled()=>return Ok(()),
            Err(error)=>return Err(error),
        },
    };
    let mut monitor_error = None;
    let first = request(original, |done| Job::Install(interface.to_string(), done)).await;
    let initial = match first {
        Ok(mut snapshot) => {
            match acquire_monitor(original).await {
                Ok(()) => {}
                Err(source) if strict(original) => {
                    publish_ready(original, Err(source.clone()));
                    return Err(channel(source));
                }
                Err(source) => {
                    monitor_error = Some(source.to_string());
                    snapshot.error = Some(source.to_string());
                    snapshot.healthy = false;
                }
            }
            Ok(snapshot)
        }
        Err(source) => Err(source),
    };
    publish_ready(original, initial.clone());
    let mut snapshot = initial.map_err(channel)?;
    publish(original, &snapshot, ReconcileTrigger::Retry);
    let mut retry = Some(0usize);
    loop {
        let mut acknowledgement = None;
        let trigger = tokio::select! { biased;
            _ = original.cancel.cancelled()=>return Ok(()),
            request=recovery.recv(), if !recovery.is_closed()=>{acknowledgement=request;ReconcileTrigger::Manual},
            changed=monitor_changed(original)=>{
                if let Err(source)=changed {
                    monitor_error=Some(source.to_string());
                    snapshot.error=Some(source.to_string());snapshot.healthy=false;
                    publish(original,&snapshot,ReconcileTrigger::Retry);
                    // Another registration is forbidden until this original
                    // has explicitly closed, even after a receive error.
                    original.monitor_io.lock().await.close().map_err(|source| channel(Arc::new(source)))?;
                }
                ReconcileTrigger::PlatformEvent
            },
            _=tokio::time::sleep(retry.map_or(ROUTE_WATCHDOG_INTERVAL,|index|ROUTE_RETRY_DELAYS[index]))=>{
                if retry.is_some(){ReconcileTrigger::Retry}else{ReconcileTrigger::Watchdog}
            },
        };
        if trigger.should_debounce() {
            tokio::select! {biased;_ = original.cancel.cancelled()=>return Ok(()),_ = tokio::time::sleep(ROUTE_EVENT_DEBOUNCE)=>{}}
            let mut monitor = original.monitor_io.lock().await;
            if !monitor.is_closed() {
                if let Err(source) = monitor.coalesce() {
                    monitor
                        .close()
                        .map_err(|source| channel(Arc::new(source)))?;
                    monitor_error = Some(source.to_string());
                    snapshot.error = Some(source.to_string());
                    snapshot.healthy = false;
                }
            }
        }
        if original.monitor_io.lock().await.is_closed() {
            monitor_error = acquire_monitor(original)
                .await
                .err()
                .map(|source| source.to_string());
        }
        match request(original, |done| {
            Job::Reconcile(original.proxy.engine().config(), done)
        })
        .await
        {
            Ok(current) => {
                snapshot = current;
                if let Some(source) = &monitor_error {
                    snapshot.error = Some(source.clone());
                    snapshot.healthy = false;
                }
                publish(original, &snapshot, trigger);
                acknowledge_recovery(
                    &mut acknowledgement,
                    if snapshot.healthy {
                        Ok(())
                    } else {
                        Err(snapshot
                            .error
                            .clone()
                            .unwrap_or_else(|| "TUN route recovery remains incomplete".into()))
                    },
                );
                retry = if snapshot.healthy {
                    None
                } else {
                    Some(next_retry(retry))
                };
            }
            Err(source) => {
                snapshot.error = Some(source.to_string());
                snapshot.healthy = false;
                publish(original, &snapshot, ReconcileTrigger::Retry);
                acknowledge_recovery(&mut acknowledgement, Err(source.to_string()));
                retry = Some(next_retry(retry));
            }
        }
    }
}
async fn request(
    original: &Shared,
    job: impl FnOnce(oneshot::Sender<Result<Snapshot, SourceError>>) -> Job,
) -> Result<Snapshot, SourceError> {
    let (completed, result) = oneshot::channel();
    original
        .jobs
        .send(job(completed))
        .await
        .map_err(|mpsc::error::SendError(_)| {
            Arc::new(io::Error::other(mpsc::error::SendError(())))
        })?;
    tokio::select! {biased;
        _=original.cancel.cancelled()=>Err(Arc::new(io::Error::new(io::ErrorKind::Interrupted,"original route operation stopped"))),
        result=result=>result.map_err(|source|Arc::new(io::Error::other(source)))?,
    }
}
async fn acquire_monitor(original: &Shared) -> Result<(), SourceError> {
    let mut monitor = original.monitor_io.lock().await;
    if monitor.is_closed() {
        *monitor = RouteChangeMonitor::prepare();
    }
    match monitor.acquire() {
        Ok(()) => Ok(()),
        Err(source) => {
            let source = Arc::new(source);
            monitor.close().map_err(Arc::new)?;
            Err(source)
        }
    }
}
async fn monitor_changed(original: &Shared) -> io::Result<()> {
    let mut monitor = original.monitor_io.lock().await;
    if monitor.is_closed() {
        std::future::pending().await
    } else {
        monitor.changed().await
    }
}
fn strict(original: &Shared) -> bool {
    original
        .native
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .spec
        .strict_route
}
fn publish_ready(original: &Shared, result: Result<Snapshot, SourceError>) {
    original
        .observed
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .ready = Some(result);
    original.changed.notify_waiters();
}
fn channel(source: SourceError) -> ChannelError {
    error(Reason::Io, "original route monitor failed").with_source(SharedError(source))
}
fn publish(original: &Shared, snapshot: &Snapshot, trigger: ReconcileTrigger) {
    let native = original
        .native
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let spec = &native.spec;
    if snapshot.healthy && trigger.invalidates_network(snapshot.changed) {
        let before = original.proxy.egress_interface.generation();
        original.proxy.egress_interface.invalidate_network();
        crate::inbound::tun::log_tun_generation_change(
            &original.proxy.egress_interface,
            before,
            "source_route_reconcile",
            Some(snapshot.changed),
        );
    }
    let mut info = original.proxy.tun_info.lock().unwrap();
    let Some(info) = info.as_mut().filter(|info| info.id == spec.id) else {
        return;
    };
    info.egress_interface_v4 = snapshot.v4.clone();
    info.egress_interface_v6 = snapshot.v6.clone();
    info.egress_interface = if spec.primary_ipv6 {
        snapshot.v6.clone().or_else(|| snapshot.v4.clone())
    } else {
        snapshot.v4.clone().or_else(|| snapshot.v6.clone())
    };
    info.route_exclusions = snapshot.exclusions.clone();
    info.healthy = snapshot.healthy;
    info.last_error = snapshot.error.clone();
    if spec.strict_route && !snapshot.healthy {
        let (v4, v6) = spec.managed_families();
        if v4 {
            original.proxy.egress_interface.mark_unavailable_for(
                false,
                snapshot
                    .error
                    .as_deref()
                    .unwrap_or("TUN routes unavailable"),
            );
        }
        if v6 {
            original.proxy.egress_interface.mark_unavailable_for(
                true,
                snapshot
                    .error
                    .as_deref()
                    .unwrap_or("TUN routes unavailable"),
            );
        }
    }
}

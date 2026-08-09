use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{watch, Notify};
use tokio::time::{interval, MissedTickBehavior};
use tracing::{debug, info};

use crate::protocol_registry::TcpRuntimeServices;
use crate::runtime::outbound_probe::{
    OutboundProbeRequest, OutboundProbeRuntime, MAX_CONCURRENT_OUTBOUND_PROBES,
};
use zero_engine::{EngineError, ProbeTrigger, ProbeTriggerAck, TargetId};

mod refresh;

#[derive(Default)]
struct ProbeOperationState {
    current: Option<String>,
    pending_manual: Option<String>,
}

impl ProbeOperationState {
    fn request(&mut self, requested: String) -> ProbeTriggerAck {
        if let Some(operation_id) = self.current.as_ref().or(self.pending_manual.as_ref()) {
            return ProbeTriggerAck {
                operation_id: operation_id.clone(),
                coalesced: true,
            };
        }
        self.pending_manual = Some(requested.clone());
        ProbeTriggerAck {
            operation_id: requested,
            coalesced: false,
        }
    }

    fn take_pending_manual(&mut self) -> Option<String> {
        self.pending_manual.take()
    }
}

fn generated_operation_id() -> String {
    static NEXT_OPERATION: AtomicU64 = AtomicU64::new(1);
    format!(
        "probe-{:016x}-{:016x}",
        unix_timestamp_ms(),
        NEXT_OPERATION.fetch_add(1, Ordering::Relaxed)
    )
}

#[derive(Clone)]
pub(crate) struct UrlTestRuntime {
    services: TcpRuntimeServices,
    outbound_probe: OutboundProbeRuntime,
}

impl UrlTestRuntime {
    pub(crate) fn new(services: TcpRuntimeServices) -> Self {
        let outbound_probe = OutboundProbeRuntime::new(services.clone());
        Self {
            services,
            outbound_probe,
        }
    }

    pub(crate) fn group_ids(&self) -> Vec<TargetId> {
        self.services.snapshot().plan().urltest_groups().to_vec()
    }

    pub(crate) fn clear_probe_triggers(&self) {
        self.services.engine().probe_trigger_registry().clear();
        self.outbound_probe.clear_shared();
    }

    pub(crate) async fn run_urltest_group(
        &self,
        group_id: TargetId,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), EngineError> {
        let plan = self.services.snapshot().plan();
        let group = plan
            .target(group_id)
            .expect("engine plan should resolve urltest group");
        let Some(urltest) = group.as_urltest() else {
            return Ok(());
        };
        let group_tag = group.tag().to_owned();
        let interval_seconds = urltest.interval().as_secs();
        let probe = OutboundProbeRequest::parse(urltest.url()).map_err(|error| {
            EngineError::InvalidUrlTestGroup {
                tag: group_tag.clone(),
                message: error.message().to_owned(),
            }
        })?;

        let probe_notify = Arc::new(Notify::new());
        let probe_operations = Arc::new(Mutex::new(ProbeOperationState::default()));
        let trigger = ProbeTrigger::new({
            let notify = Arc::clone(&probe_notify);
            let operations = Arc::clone(&probe_operations);
            move |requested_operation_id| {
                let ack = operations
                    .lock()
                    .expect("urltest probe operation lock poisoned")
                    .request(requested_operation_id);
                if !ack.coalesced {
                    notify.notify_one();
                }
                ack
            }
        });
        self.services
            .engine()
            .probe_trigger_registry()
            .register(&group_tag, trigger);

        info!(
            group_tag = %group_tag,
            url = probe.url.as_str(),
            interval_seconds,
            max_concurrent_probes = MAX_CONCURRENT_OUTBOUND_PROBES,
            concurrency_scope = "process",
            "urltest group started"
        );

        let mut schedule = interval(Duration::from_secs(interval_seconds));
        schedule.set_missed_tick_behavior(MissedTickBehavior::Skip);
        schedule.tick().await;
        self.run_probe_operation(
            group_id,
            &probe,
            "startup",
            generated_operation_id(),
            probe_operations.as_ref(),
        )
        .await;
        schedule.reset();

        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    match changed {
                        Ok(()) if *shutdown.borrow() => break,
                        Ok(()) => {}
                        Err(_) => break,
                    }
                }
                _ = probe_notify.notified() => {
                    debug!(group_tag = %group_tag, "urltest probe triggered by api");
                    let operation_id = probe_operations
                        .lock()
                        .expect("urltest probe operation lock poisoned")
                        .take_pending_manual();
                    if let Some(operation_id) = operation_id {
                        self.run_probe_operation(
                            group_id,
                            &probe,
                            "manual",
                            operation_id,
                            probe_operations.as_ref(),
                        ).await;
                    }
                    schedule.reset();
                }
                _ = schedule.tick() => {
                    self.run_probe_operation(
                        group_id,
                        &probe,
                        "scheduled",
                        generated_operation_id(),
                        probe_operations.as_ref(),
                    ).await;
                    schedule.reset();
                }
            }
        }

        self.services
            .engine()
            .probe_trigger_registry()
            .remove(&group_tag);
        info!(group_tag = %group_tag, "urltest group stopped");
        Ok(())
    }

    async fn run_probe_operation(
        &self,
        group_id: TargetId,
        probe: &OutboundProbeRequest,
        trigger: &'static str,
        operation_id: String,
        operations: &Mutex<ProbeOperationState>,
    ) {
        {
            let mut state = operations
                .lock()
                .expect("urltest probe operation lock poisoned");
            state.current = Some(operation_id.clone());
        }
        self.refresh_urltest_group(group_id, probe, trigger, &operation_id)
            .await;
        let mut state = operations
            .lock()
            .expect("urltest probe operation lock poisoned");
        if state.current.as_deref() == Some(operation_id.as_str()) {
            state.current = None;
        }
    }
}

fn unix_timestamp_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_millis() as u64
}

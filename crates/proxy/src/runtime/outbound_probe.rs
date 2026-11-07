use std::collections::{HashMap, HashSet};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex, OnceLock,
};

use futures_util::future::{BoxFuture, FutureExt, Shared};
use zero_engine::{ResolvedOutbound, TargetId};

use crate::protocol_registry::TcpRuntimeServices;

mod execution;
mod model;

pub(crate) use model::{OutboundProbeError, OutboundProbeRequest};

pub(crate) const MAX_CONCURRENT_OUTBOUND_PROBES: usize = zero_runtime::MAX_PROBE_OPERATIONS;
const OUTBOUND_PROBE_TRANSPORT_TIMEOUT_MS: u64 = 5_000;
const OUTBOUND_PROBE_MAX_TIMEOUT_MS: u64 = 60_000;

type SharedProbeFuture = Shared<BoxFuture<'static, Result<u64, OutboundProbeError>>>;

mod shared;
use shared::{ProbeOriginal, SharedProbeEntry, SharedProbeWaiter};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct OutboundProbeDeadline {
    pub(crate) dns_budget_ms: u64,
    pub(crate) transport_budget_ms: u64,
    pub(crate) timeout_ms: u64,
    pub(crate) capped: bool,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ProbeKey {
    host_identity: usize,
    config_identity: usize,
    target_tag: String,
    url: String,
    intent: crate::runtime::tcp_dispatch::TcpDispatchIntent,
}

#[derive(Clone)]
pub(crate) struct OutboundProbeRuntime {
    services: TcpRuntimeServices,
    shared_probes: Arc<Mutex<HashMap<ProbeKey, SharedProbeEntry>>>,
}

impl OutboundProbeRuntime {
    pub(crate) fn new(services: TcpRuntimeServices) -> Self {
        Self {
            services,
            shared_probes: global_shared_probes(),
        }
    }

    pub(crate) fn clear_shared(&self) {
        let host_identity = Arc::as_ptr(&self.services.protocols().runtime_host()) as usize;
        let mut probes = self
            .shared_probes
            .lock()
            .expect("shared outbound probe lock poisoned");
        let keys: Vec<_> = probes
            .keys()
            .filter(|key| key.host_identity == host_identity)
            .cloned()
            .collect();
        let removed: Vec<_> = keys
            .into_iter()
            .filter_map(|key| probes.remove(&key))
            .collect();
        drop(probes);
        drop(removed);
    }

    /// Probe deadline budget, including node-DNS fallbacks and its hard cap.
    pub(crate) fn probe_deadline(&self) -> OutboundProbeDeadline {
        probe_deadline_for_dns(self.services.snapshot().config().runtime.dns.as_ref())
    }

    pub(crate) async fn probe_target_tag(
        &self,
        target_tag: &str,
        request: &OutboundProbeRequest,
    ) -> Result<u64, OutboundProbeError> {
        let target_id = self
            .services
            .snapshot()
            .plan()
            .target_id(target_tag)
            .ok_or_else(|| {
                OutboundProbeError::new(
                    "target_not_found",
                    format!("outbound probe target `{target_tag}` was not found"),
                )
            })?;
        self.probe_target_with_intent(
            target_id,
            request,
            crate::runtime::tcp_dispatch::TcpDispatchIntent::DiagnosticProbe,
            None,
        )
        .await
    }

    pub(crate) async fn probe_target_tag_until(
        &self,
        target_tag: &str,
        request: &OutboundProbeRequest,
        stop: &zero_platform_tokio::contract::CancellationToken,
    ) -> Result<u64, OutboundProbeError> {
        let target_id = self
            .services
            .snapshot()
            .plan()
            .target_id(target_tag)
            .ok_or_else(|| {
                OutboundProbeError::new(
                    "target_not_found",
                    format!("outbound probe target `{target_tag}` was not found"),
                )
            })?;
        self.probe_target_with_intent(
            target_id,
            request,
            crate::runtime::tcp_dispatch::TcpDispatchIntent::DiagnosticProbe,
            Some(stop),
        )
        .await
    }

    pub(crate) async fn probe_target_shared(
        &self,
        target_id: TargetId,
        request: &OutboundProbeRequest,
    ) -> Result<u64, OutboundProbeError> {
        self.probe_target_with_intent(
            target_id,
            request,
            crate::runtime::tcp_dispatch::TcpDispatchIntent::PolicyProbe,
            None,
        )
        .await
    }

    async fn probe_target_with_intent(
        &self,
        target_id: TargetId,
        request: &OutboundProbeRequest,
        intent: crate::runtime::tcp_dispatch::TcpDispatchIntent,
        stop: Option<&zero_platform_tokio::contract::CancellationToken>,
    ) -> Result<u64, OutboundProbeError> {
        let target_tag = self.target_tag(target_id).ok_or_else(|| {
            OutboundProbeError::new(
                "target_resolution_failed",
                "failed to resolve outbound probe target",
            )
        })?;
        let key = ProbeKey {
            host_identity: Arc::as_ptr(&self.services.protocols().runtime_host()) as usize,
            config_identity: Arc::as_ptr(self.services.snapshot().config()) as usize,
            target_tag,
            url: request.url.clone(),
            intent,
        };
        // Existing coalesced calls require no second credit. New map entries
        // are bounded before publication, including calls queued for a task.
        let existing = {
            let mut probes = self
                .shared_probes
                .lock()
                .expect("shared outbound probe lock poisoned");
            probes.get_mut(&key).map(|entry| {
                entry.waiters += 1;
                (entry.future.clone(), entry.original.clone())
            })
        };
        let (future, original) = if let Some(existing) = existing {
            existing
        } else {
            let reserve = zero_runtime::reserve_probe();
            tokio::pin!(reserve);
            let credit = match stop {
                Some(stop) => tokio::select! { biased;
                    _ = stop.cancelled() => return Err(cancelled_probe()),
                    credit = &mut reserve => credit,
                },
                None => reserve.await,
            }
            .map_err(|source| OutboundProbeError::new("probe_unavailable", source.to_string()))?;
            let mut probes = self
                .shared_probes
                .lock()
                .expect("shared outbound probe lock poisoned");
            if let Some(existing) = probes.get_mut(&key) {
                existing.waiters += 1;
                let future = (existing.future.clone(), existing.original.clone());
                drop(probes);
                drop(credit);
                future
            } else {
                let runtime = self.clone();
                let request = request.clone();
                let original = Arc::new(ProbeOriginal::default());
                let accepted = original.clone();
                let future = async move {
                    runtime
                        .run_accepted_probe(target_id, request, intent, credit, accepted)
                        .await
                }
                .boxed()
                .shared();
                probes.insert(
                    key.clone(),
                    SharedProbeEntry {
                        future: future.clone(),
                        waiters: 1,
                        original: original.clone(),
                    },
                );
                (future, original)
            }
        };
        let mut waiter = SharedProbeWaiter {
            probes: self.shared_probes.clone(),
            key,
            future,
            original,
            attached: true,
        };

        let result = match stop {
            Some(stop) => tokio::select! { biased;
                _ = stop.cancelled() => {
                    // Stop this caller immediately. Only the last coalesced
                    // caller cancels the same actual host call. Keep polling
                    // its original future through real Native/Scope cleanup.
                    waiter.detach();
                    waiter.future.clone().await
                },
                result = waiter.future.clone() => result,
            },
            None => waiter.future.clone().await,
        };
        drop(waiter);
        result
    }

    fn resolve_target_id(
        &self,
        target_id: TargetId,
    ) -> Option<(ResolvedOutbound<'static>, Arc<zero_engine::EnginePlan>)> {
        self.services
            .engine()
            .resolve_target_id_in_snapshot(self.services.snapshot(), target_id)
    }

    fn target_tag(&self, target_id: TargetId) -> Option<String> {
        self.services
            .engine()
            .target_tag_in_snapshot(self.services.snapshot(), target_id)
    }
}

fn cancelled_probe() -> OutboundProbeError {
    OutboundProbeError::from_engine(
        std::io::Error::new(
            std::io::ErrorKind::Interrupted,
            "diagnostic caller cancelled",
        )
        .into(),
    )
}

fn probe_deadline_for_dns(dns: Option<&zero_config::DnsConfig>) -> OutboundProbeDeadline {
    let dns_budget_ms = dns.map_or(0, |dns| {
        let policy = &dns.policy;
        let mut tags = Vec::new();
        if let Some(server) = policy.node_server.as_deref() {
            tags.push(server);
            tags.extend(policy.node_fallback_servers.iter().map(String::as_str));
        } else {
            tags.push(dns.default_server.as_str());
            tags.extend(policy.fallback_servers.iter().map(String::as_str));
        }
        let mut seen = HashSet::new();
        tags.into_iter()
            .filter(|tag| seen.insert(*tag))
            .fold(0_u64, |total, tag| {
                total.saturating_add(policy.timeout_ms_for(tag))
            })
    });
    let requested_timeout_ms = OUTBOUND_PROBE_TRANSPORT_TIMEOUT_MS.saturating_add(dns_budget_ms);
    let timeout_ms = requested_timeout_ms.clamp(
        OUTBOUND_PROBE_TRANSPORT_TIMEOUT_MS,
        OUTBOUND_PROBE_MAX_TIMEOUT_MS,
    );
    OutboundProbeDeadline {
        dns_budget_ms,
        transport_budget_ms: OUTBOUND_PROBE_TRANSPORT_TIMEOUT_MS,
        timeout_ms,
        capped: timeout_ms < requested_timeout_ms,
    }
}

fn global_shared_probes() -> Arc<Mutex<HashMap<ProbeKey, SharedProbeEntry>>> {
    static PROBES: OnceLock<Arc<Mutex<HashMap<ProbeKey, SharedProbeEntry>>>> = OnceLock::new();
    PROBES
        .get_or_init(|| Arc::new(Mutex::new(HashMap::new())))
        .clone()
}

#[cfg(test)]
mod tests;

#[cfg(test)]
#[path = "outbound_probe/ownership_tests.rs"]
mod ownership_tests;

#[cfg(test)]
#[path = "outbound_probe/control_stop_tests.rs"]
mod control_stop_tests;

mod target;

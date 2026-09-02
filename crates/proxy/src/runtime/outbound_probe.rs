use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use futures_util::future::{BoxFuture, FutureExt, Shared};
use tokio::sync::Semaphore;
use tokio::time::timeout;
use zero_core::{Address, Network, ProtocolType, Session};
use zero_engine::{EngineError, ResolvedOutbound, TargetId};
use zero_traits::AsyncSocket;

use crate::protocol_registry::TcpRuntimeServices;
use crate::transport::extract_tcp_stream;

mod model;

pub(crate) use model::{OutboundProbeError, OutboundProbeRequest};

pub(crate) const MAX_CONCURRENT_OUTBOUND_PROBES: usize = 8;
const OUTBOUND_PROBE_TRANSPORT_TIMEOUT_MS: u64 = 5_000;
const OUTBOUND_PROBE_MAX_TIMEOUT_MS: u64 = 60_000;

type SharedProbeFuture = Shared<BoxFuture<'static, Result<u64, OutboundProbeError>>>;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ProbeKey {
    config_identity: usize,
    target_tag: String,
    url: String,
    intent: crate::runtime::tcp_dispatch::TcpDispatchIntent,
}

#[derive(Clone)]
pub(crate) struct OutboundProbeRuntime {
    services: TcpRuntimeServices,
    limiter: Arc<Semaphore>,
    shared_probes: Arc<Mutex<HashMap<ProbeKey, SharedProbeFuture>>>,
}

impl OutboundProbeRuntime {
    pub(crate) fn new(services: TcpRuntimeServices) -> Self {
        Self {
            services,
            limiter: global_probe_limiter(),
            shared_probes: global_shared_probes(),
        }
    }

    pub(crate) fn clear_shared(&self) {
        self.shared_probes
            .lock()
            .expect("shared outbound probe lock poisoned")
            .clear();
    }

    /// Total probe deadline, including the complete node-DNS fallback chain.
    pub(crate) fn probe_timeout_ms(&self) -> u64 {
        probe_timeout_ms_for_dns(self.services.snapshot().config().runtime.dns.as_ref())
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
        )
        .await
    }

    async fn probe_target_with_intent(
        &self,
        target_id: TargetId,
        request: &OutboundProbeRequest,
        intent: crate::runtime::tcp_dispatch::TcpDispatchIntent,
    ) -> Result<u64, OutboundProbeError> {
        let target_tag = self.target_tag(target_id).ok_or_else(|| {
            OutboundProbeError::new(
                "target_resolution_failed",
                "failed to resolve outbound probe target",
            )
        })?;
        let key = ProbeKey {
            config_identity: Arc::as_ptr(self.services.snapshot().config()) as usize,
            target_tag,
            url: request.url.clone(),
            intent,
        };
        let shared = {
            let mut probes = self
                .shared_probes
                .lock()
                .expect("shared outbound probe lock poisoned");
            if let Some(existing) = probes.get(&key) {
                existing.clone()
            } else {
                let runtime = self.clone();
                let request = request.clone();
                let future = async move {
                    let _permit = runtime.limiter.clone().acquire_owned().await.map_err(|_| {
                        OutboundProbeError::new(
                            "probe_unavailable",
                            "outbound probe concurrency limiter is unavailable",
                        )
                    })?;
                    let Some((resolved, _plan)) = runtime.resolve_target_id(target_id) else {
                        return Err(OutboundProbeError::new(
                            "target_resolution_failed",
                            "failed to resolve outbound probe target",
                        ));
                    };
                    runtime
                        .probe_resolved_outbound(resolved, &request, intent)
                        .await
                }
                .boxed()
                .shared();
                probes.insert(key.clone(), future.clone());
                future
            }
        };

        let result = shared.await;
        self.shared_probes
            .lock()
            .expect("shared outbound probe lock poisoned")
            .remove(&key);
        result
    }

    async fn probe_resolved_outbound(
        &self,
        resolved: ResolvedOutbound<'static>,
        request: &OutboundProbeRequest,
        intent: crate::runtime::tcp_dispatch::TcpDispatchIntent,
    ) -> Result<u64, OutboundProbeError> {
        if matches!(resolved, ResolvedOutbound::Relay { .. }) {
            return Err(OutboundProbeError::new(
                "unsupported_target",
                "relay chain cannot be used as an outbound latency probe target",
            ));
        }

        let timeout_ms = self.probe_timeout_ms();
        match timeout(Duration::from_millis(timeout_ms), async {
            let started_at = Instant::now();
            let session = Session::new(
                0,
                Address::Domain(request.host.clone()),
                request.port,
                Network::Tcp,
                ProtocolType::UNKNOWN,
            );
            let outbound = crate::runtime::tcp_dispatch::dispatch_tcp_outbound(
                self.services.clone(),
                &session,
                resolved,
                intent,
            )
            .await
            .map_err(|failure| failure.error)?;
            let result = extract_tcp_stream(outbound)?;
            let mut socket = result.upstream;
            socket
                .write_all(request.request.as_bytes())
                .await
                .map_err(EngineError::from)?;

            let mut response = [0_u8; 1];
            let read = socket
                .read(&mut response)
                .await
                .map_err(EngineError::from)?;
            if read == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "outbound probe target closed without an HTTP response",
                )
                .into());
            }
            Ok(started_at.elapsed().as_millis() as u64)
        })
        .await
        {
            Ok(result) => result.map_err(OutboundProbeError::from_engine),
            Err(_) => Err(OutboundProbeError::new(
                "probe_timeout",
                format!("outbound latency probe timed out after {timeout_ms} ms"),
            )),
        }
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

fn probe_timeout_ms_for_dns(dns: Option<&zero_config::DnsConfig>) -> u64 {
    let Some(dns) = dns else {
        return OUTBOUND_PROBE_TRANSPORT_TIMEOUT_MS;
    };
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
    let dns_budget = tags
        .into_iter()
        .filter(|tag| seen.insert(*tag))
        .fold(0_u64, |total, tag| {
            total.saturating_add(policy.timeout_ms_for(tag))
        });
    OUTBOUND_PROBE_TRANSPORT_TIMEOUT_MS
        .saturating_add(dns_budget)
        .clamp(
            OUTBOUND_PROBE_TRANSPORT_TIMEOUT_MS,
            OUTBOUND_PROBE_MAX_TIMEOUT_MS,
        )
}

fn global_probe_limiter() -> Arc<Semaphore> {
    static LIMITER: OnceLock<Arc<Semaphore>> = OnceLock::new();
    LIMITER
        .get_or_init(|| Arc::new(Semaphore::new(MAX_CONCURRENT_OUTBOUND_PROBES)))
        .clone()
}

fn global_shared_probes() -> Arc<Mutex<HashMap<ProbeKey, SharedProbeFuture>>> {
    static PROBES: OnceLock<Arc<Mutex<HashMap<ProbeKey, SharedProbeFuture>>>> = OnceLock::new();
    PROBES
        .get_or_init(|| Arc::new(Mutex::new(HashMap::new())))
        .clone()
}

#[cfg(test)]
mod tests;

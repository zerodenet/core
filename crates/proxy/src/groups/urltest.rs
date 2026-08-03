use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use futures_util::future::{BoxFuture, FutureExt, Shared};
use futures_util::stream::{self, StreamExt};
use tokio::sync::{watch, Notify, Semaphore};
use tokio::time::{interval, timeout, MissedTickBehavior};
use tracing::{debug, info, warn};
use zero_core::{Address, Network, ProtocolType, Session};
use zero_traits::AsyncSocket;

use crate::protocol_registry::TcpRuntimeServices;
use crate::transport::extract_tcp_stream;
use zero_engine::{
    EngineError, PolicyProbeCompletedPayload, PolicyProbeMember, ProbeTrigger, ResolvedOutbound,
    TargetId, UrlTestMemberState,
};

use super::super::logging::log_urltest_group_target_changed;

const MAX_CONCURRENT_URLTEST_PROBES: usize = 8;

type SharedProbeFuture = Shared<BoxFuture<'static, Result<u64, String>>>;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ProbeKey {
    config_identity: usize,
    target_tag: String,
    url: String,
}

fn global_probe_limiter() -> Arc<Semaphore> {
    static LIMITER: OnceLock<Arc<Semaphore>> = OnceLock::new();
    LIMITER
        .get_or_init(|| Arc::new(Semaphore::new(MAX_CONCURRENT_URLTEST_PROBES)))
        .clone()
}

fn global_shared_probes() -> Arc<Mutex<HashMap<ProbeKey, SharedProbeFuture>>> {
    static PROBES: OnceLock<Arc<Mutex<HashMap<ProbeKey, SharedProbeFuture>>>> = OnceLock::new();
    PROBES
        .get_or_init(|| Arc::new(Mutex::new(HashMap::new())))
        .clone()
}

#[derive(Clone)]
pub(crate) struct UrlTestRuntime {
    services: TcpRuntimeServices,
    probe_limiter: Arc<Semaphore>,
    shared_probes: Arc<Mutex<HashMap<ProbeKey, SharedProbeFuture>>>,
}

impl UrlTestRuntime {
    pub(crate) fn new(services: TcpRuntimeServices) -> Self {
        Self {
            services,
            probe_limiter: global_probe_limiter(),
            shared_probes: global_shared_probes(),
        }
    }

    pub(crate) fn group_ids(&self) -> Vec<TargetId> {
        self.services.snapshot().plan().urltest_groups().to_vec()
    }

    pub(crate) fn clear_probe_triggers(&self) {
        self.services.engine().probe_trigger_registry().clear();
        self.shared_probes
            .lock()
            .expect("shared urltest probe lock poisoned")
            .clear();
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
        let probe = UrlTestProbe::parse(urltest.url()).map_err(|message| {
            EngineError::InvalidUrlTestGroup {
                tag: group_tag.clone(),
                message,
            }
        })?;

        let probe_notify = Arc::new(Notify::new());
        let probe_running = Arc::new(AtomicBool::new(false));
        let trigger = ProbeTrigger::new({
            let notify = Arc::clone(&probe_notify);
            let running = Arc::clone(&probe_running);
            move || {
                // A completion already in progress is a fresh authoritative
                // result for a concurrent manual click. Do not enqueue another
                // full cycle behind it and probe every member twice.
                if !running.load(Ordering::Acquire) {
                    notify.notify_one();
                }
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
            max_concurrent_probes = MAX_CONCURRENT_URLTEST_PROBES,
            concurrency_scope = "process",
            "urltest group started"
        );

        let mut schedule = interval(Duration::from_secs(interval_seconds));
        schedule.set_missed_tick_behavior(MissedTickBehavior::Skip);
        schedule.tick().await;
        self.refresh_urltest_group_guarded(group_id, &probe, "startup", probe_running.as_ref())
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
                    self.refresh_urltest_group_guarded(
                        group_id,
                        &probe,
                        "manual",
                        probe_running.as_ref(),
                    ).await;
                    schedule.reset();
                }
                _ = schedule.tick() => {
                    self.refresh_urltest_group_guarded(
                        group_id,
                        &probe,
                        "scheduled",
                        probe_running.as_ref(),
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

    async fn refresh_urltest_group_guarded(
        &self,
        group_id: TargetId,
        probe: &UrlTestProbe,
        trigger: &'static str,
        running: &AtomicBool,
    ) {
        if running.swap(true, Ordering::AcqRel) {
            return;
        }
        self.refresh_urltest_group(group_id, probe, trigger).await;
        running.store(false, Ordering::Release);
    }

    async fn refresh_urltest_group(
        &self,
        group_id: TargetId,
        probe: &UrlTestProbe,
        trigger: &'static str,
    ) {
        let plan = self.services.snapshot().plan();
        let Some(group) = plan.target(group_id) else {
            debug!(
                group_id = group_id.index(),
                trigger, "urltest group disappeared during config reload"
            );
            return;
        };
        let Some(urltest) = group.as_urltest() else {
            debug!(
                group_id = group_id.index(),
                trigger, "urltest group changed during config reload"
            );
            return;
        };
        let group_tag = group.tag();
        let mut best: Option<ProbeSuccess> = None;
        let started_at_unix_ms = unix_timestamp_ms();
        let started_at = Instant::now();

        // Each group can prepare up to eight member futures, while every
        // UrlTestRuntime instance shares one process-wide semaphore. Multiple
        // urltest groups and diagnostics.probe_outbound therefore cannot
        // multiply the real socket concurrency beyond the global limit.
        let mut probe_results = stream::iter(urltest.members().iter().copied().enumerate())
            .map(|(index, member_id)| async move {
                let member = self
                    .target_tag(member_id)
                    .unwrap_or_else(|| "<unknown>".to_owned());
                let effective_chains = self.resolve_target_chains(member_id);

                match self.probe_target_shared(member_id, probe).await {
                    Ok(latency_ms) => (
                        index,
                        UrlTestMemberState {
                            member_id,
                            healthy: true,
                            latency_ms: Some(latency_ms),
                            last_checked_unix_ms: Some(started_at_unix_ms),
                            last_error: None,
                            effective_chains,
                        },
                        Some(ProbeSuccess {
                            outbound_id: member_id,
                            latency_ms,
                        }),
                    ),
                    Err(error) => {
                        debug!(
                            group_tag = group_tag,
                            outbound_tag = member,
                            error = %error,
                            "urltest probe failed"
                        );
                        (
                            index,
                            UrlTestMemberState {
                                member_id,
                                healthy: false,
                                latency_ms: None,
                                last_checked_unix_ms: Some(started_at_unix_ms),
                                last_error: Some(error),
                                effective_chains,
                            },
                            None,
                        )
                    }
                }
            })
            .buffer_unordered(MAX_CONCURRENT_URLTEST_PROBES)
            .collect::<Vec<_>>()
            .await;

        // Preserve configured member order in snapshots and events even though
        // the probes complete out of order.
        probe_results.sort_by_key(|(index, _, _)| *index);
        let mut member_states = Vec::with_capacity(probe_results.len());
        for (_, member_state, success) in probe_results {
            if let Some(success) = success {
                if best
                    .as_ref()
                    .map(|current| success.latency_ms < current.latency_ms)
                    .unwrap_or(true)
                {
                    best = Some(success);
                }
            }
            member_states.push(member_state);
        }

        let previous = self.urltest_selected_target(group_id);
        let Some(selected) = best
            .as_ref()
            .map(|probe| probe.outbound_id)
            .or(previous)
            .or(Some(urltest.initial_member()))
        else {
            return;
        };
        let selected_tag = self
            .target_tag(selected)
            .unwrap_or_else(|| "<unknown>".to_owned());
        let previous_tag = previous.and_then(|target| self.target_tag(target));

        let latency_ms = best
            .as_ref()
            .and_then(|probe| (probe.outbound_id == selected).then_some(probe.latency_ms));

        let probe_members: Vec<PolicyProbeMember> = member_states
            .iter()
            .map(|state| {
                let tag = self
                    .target_tag(state.member_id)
                    .unwrap_or_else(|| "<unknown>".to_owned());
                PolicyProbeMember {
                    target_tag: tag,
                    healthy: state.healthy,
                    latency_ms: state.latency_ms,
                    error: state.last_error.clone(),
                }
            })
            .collect();

        let healthy_members = member_states.iter().filter(|member| member.healthy).count();
        let total_members = member_states.len();
        self.update_urltest_state(group_id, selected, latency_ms, member_states);

        let completed_at_unix_ms = unix_timestamp_ms();
        let duration_ms = started_at.elapsed().as_millis() as u64;
        let event_payload = PolicyProbeCompletedPayload {
            policy_tag: group_tag.to_owned(),
            trigger: trigger.to_owned(),
            url: probe.url.clone(),
            started_at_unix_ms,
            completed_at_unix_ms,
            duration_ms,
            selected: Some(selected_tag.clone()),
            members: probe_members,
        };

        info!(
            event_type = "policy.probe.completed",
            group_kind = "url_test",
            group_tag,
            trigger,
            url = probe.url.as_str(),
            started_at_unix_ms,
            completed_at_unix_ms,
            duration_ms,
            selected = %selected_tag,
            healthy_members,
            total_members,
            members = ?event_payload.members,
            "urltest probe completed"
        );

        self.services
            .engine()
            .push_policy_probe_completed(group_tag, event_payload);

        log_urltest_group_target_changed(
            group_tag,
            previous_tag.as_deref(),
            &selected_tag,
            latency_ms,
        );

        if best.is_none() {
            warn!(
                group_tag = group_tag,
                selected = selected_tag,
                "urltest probe found no healthy outbound; keeping current selection"
            );
        }
    }

    pub(crate) async fn probe_outbound_single(
        &self,
        target_tag: &str,
        url: &str,
    ) -> Result<u64, EngineError> {
        let probe =
            UrlTestProbe::parse(url).map_err(|message| EngineError::InvalidUrlTestGroup {
                tag: target_tag.to_owned(),
                message,
            })?;
        let plan = self.services.snapshot().plan();
        let Some(target_id) = plan.target_id(target_tag) else {
            return Err(EngineError::SelectorGroupNotFound {
                tag: target_tag.to_owned(),
            });
        };
        self.probe_target_shared(target_id, &probe)
            .await
            .map_err(|message| EngineError::Io(std::io::Error::other(message)))
    }

    async fn probe_target_shared(
        &self,
        target_id: TargetId,
        probe: &UrlTestProbe,
    ) -> Result<u64, String> {
        let target_tag = self
            .target_tag(target_id)
            .ok_or_else(|| "failed to resolve probe target".to_owned())?;
        let config = self.services.engine().config();
        let key = ProbeKey {
            config_identity: Arc::as_ptr(&config) as usize,
            target_tag,
            url: probe.url.clone(),
        };

        let shared = {
            let mut probes = self
                .shared_probes
                .lock()
                .expect("shared urltest probe lock poisoned");
            if let Some(existing) = probes.get(&key) {
                existing.clone()
            } else {
                let runtime = self.clone();
                let probe = probe.clone();
                let future = async move {
                    let _permit = runtime
                        .probe_limiter
                        .clone()
                        .acquire_owned()
                        .await
                        .map_err(|_| "urltest probe limiter closed".to_owned())?;
                    let Some((candidate, _plan)) = runtime.resolve_target_id(target_id) else {
                        return Err("failed to resolve probe target".to_owned());
                    };
                    runtime
                        .probe_outbound(candidate, &probe)
                        .await
                        .map_err(normalize_probe_error)
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
            .expect("shared urltest probe lock poisoned")
            .remove(&key);
        result
    }

    async fn probe_outbound(
        &self,
        candidate: ResolvedOutbound<'static>,
        probe: &UrlTestProbe,
    ) -> Result<u64, EngineError> {
        match candidate {
            ResolvedOutbound::Relay { .. } => Err(EngineError::Io(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "relay chain cannot be used as a urltest member",
            ))),
            resolved => self.probe_resolved_outbound(resolved, probe).await,
        }
    }

    async fn probe_resolved_outbound(
        &self,
        resolved: ResolvedOutbound<'static>,
        probe: &UrlTestProbe,
    ) -> Result<u64, EngineError> {
        const URLTEST_PROBE_TIMEOUT: Duration = Duration::from_secs(5);

        timeout(URLTEST_PROBE_TIMEOUT, async {
            let started_at = Instant::now();
            let probe_session = Session::new(
                0,
                Address::Domain(probe.host.clone()),
                probe.port,
                Network::Tcp,
                ProtocolType::UNKNOWN,
            );

            let outbound = crate::runtime::tcp_dispatch::dispatch_tcp_outbound(
                self.services.clone(),
                &probe_session,
                resolved,
            )
            .await
            .map_err(|failure| failure.error)?;
            let result = extract_tcp_stream(outbound)?;
            let mut socket = result.upstream;

            socket
                .write_all(probe.request.as_bytes())
                .await
                .map_err(EngineError::from)?;

            let mut buf = [0_u8; 1];
            let read = socket.read(&mut buf).await.map_err(EngineError::from)?;
            if read == 0 {
                return Err(std::io::Error::other(
                    "probe target closed connection without response",
                )
                .into());
            }

            Ok(started_at.elapsed().as_millis() as u64)
        })
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "urltest probe timed out"))?
    }

    fn resolve_target_id(
        &self,
        target_id: TargetId,
    ) -> Option<(ResolvedOutbound<'static>, Arc<zero_engine::EnginePlan>)> {
        self.services
            .engine()
            .resolve_target_id_in_snapshot(self.services.snapshot(), target_id)
    }

    fn resolve_target_chains(&self, target_id: TargetId) -> Vec<Vec<TargetId>> {
        self.services
            .engine()
            .resolve_target_chains_in_snapshot(self.services.snapshot(), target_id)
    }

    fn target_tag(&self, target_id: TargetId) -> Option<String> {
        self.services
            .engine()
            .target_tag_in_snapshot(self.services.snapshot(), target_id)
    }

    fn urltest_selected_target(&self, group_id: TargetId) -> Option<TargetId> {
        self.services.engine().urltest_selected_target(group_id)
    }

    fn update_urltest_state(
        &self,
        group_id: TargetId,
        selected: TargetId,
        latency_ms: Option<u64>,
        members: Vec<UrlTestMemberState>,
    ) {
        self.services
            .engine()
            .update_urltest_state(group_id, selected, latency_ms, members);
    }
}

fn normalize_probe_error(error: EngineError) -> String {
    let message = error.to_string();
    message
        .strip_prefix("io error: ")
        .unwrap_or(message.as_str())
        .to_owned()
}

fn unix_timestamp_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_millis() as u64
}

struct ProbeSuccess {
    outbound_id: TargetId,
    latency_ms: u64,
}

#[derive(Clone)]
struct UrlTestProbe {
    url: String,
    host: String,
    port: u16,
    request: String,
}

impl UrlTestProbe {
    fn parse(url: &str) -> Result<Self, String> {
        let rest = url
            .strip_prefix("http://")
            .ok_or_else(|| "urltest currently only supports `http://` probe urls".to_owned())?;

        let (authority, path) = match rest.split_once('/') {
            Some((authority, suffix)) => (authority, format!("/{}", suffix)),
            None => (rest, "/".to_owned()),
        };

        if authority.trim().is_empty() {
            return Err("urltest probe url requires a host".to_owned());
        }

        let (host, port) = parse_authority(authority)?;
        let host_header = if port == 80 {
            authority.to_owned()
        } else if authority.contains(':') && !authority.starts_with('[') {
            format!("{host}:{port}")
        } else {
            authority.to_owned()
        };

        let request =
            format!("HEAD {path} HTTP/1.1\r\nHost: {host_header}\r\nConnection: close\r\n\r\n");

        Ok(Self {
            url: url.to_owned(),
            host,
            port,
            request,
        })
    }
}

fn parse_authority(authority: &str) -> Result<(String, u16), String> {
    if let Some(rest) = authority.strip_prefix('[') {
        let (host, port_part) = rest
            .split_once(']')
            .ok_or_else(|| "invalid bracketed host in urltest probe url".to_owned())?;
        let port = match port_part.strip_prefix(':') {
            Some(port) => port
                .parse::<u16>()
                .map_err(|_| "invalid port in urltest probe url".to_owned())?,
            None if port_part.is_empty() => 80,
            _ => return Err("invalid authority in urltest probe url".to_owned()),
        };
        return Ok((host.to_owned(), port));
    }

    match authority.rsplit_once(':') {
        Some((host, port)) if !host.contains(':') => Ok((
            host.to_owned(),
            port.parse::<u16>()
                .map_err(|_| "invalid port in urltest probe url".to_owned())?,
        )),
        _ => Ok((authority.to_owned(), 80)),
    }
}

//! One real job, its declared reservations and the existing one startup actor.
use super::*;
use tokio::time::{Duration, Instant};
use zero_platform_tokio::{
    contract::scope::{TaskJoinWitness, TaskRegistrar},
    owned_process::{OwnedProcess, ProcessEndpointHandoff},
};
pub(in crate::transport::plugin) struct PendingEndpoint {
    pub(in crate::transport::plugin) endpoint: Arc<ProcessEndpointHandoff>,
    pub(in crate::transport::plugin) process: Option<Arc<OwnedProcess>>,
}
impl std::fmt::Debug for PendingEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingPluginEndpoint")
            .field("instance", &self.endpoint.instance())
            .field("process_installed", &self.process.is_some())
            .finish_non_exhaustive()
    }
}
struct Attempt(Arc<PluginLease>, bool);
impl Drop for Attempt {
    fn drop(&mut self) {
        if self.1 {
            let _ = self.0.child.lock().unwrap().start_kill();
            if let Some(endpoint) = &self.0.process_endpoint {
                endpoint.request_stop();
            }
        }
    }
}
impl PluginPlan {
    pub(super) async fn acquire_endpoint(
        &self,
        tcp: bool,
        resources: Arc<dyn zero_platform_tokio::contract::component::ResourceAccess>,
        claimed: Arc<std::sync::atomic::AtomicBool>,
        tasks: TaskRegistrar,
        actor: TaskJoinWitness,
        ordinal: usize,
        end: Instant,
    ) -> io::Result<Option<Arc<PluginLease>>> {
        if (tcp && !self.supports_tcp()) || (!tcp && !self.supports_udp()) {
            return Ok(None);
        }
        let mut slot = tokio::select! {biased;_=self.cancel.cancelled()=>return Err(revoked()),slot=self.running.lock()=>slot};
        if self.cancel.is_cancelled() {
            return Err(revoked());
        }
        if slot.is_some() || self.pending_endpoint.lock().unwrap().is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "same plugin process already has a lifecycle owner",
            ));
        }
        // Host charges and strongly installs its actual cold job and actor before
        // either reservation bind. The same Plan retains it before any await.
        let endpoint = resources
            .reserve_process_endpoint(ordinal, &tasks, &actor)
            .map_err(io::Error::other)?;
        claimed.store(true, std::sync::atomic::Ordering::Release);
        *self.pending_endpoint.lock().unwrap() = Some(PendingEndpoint {
            endpoint: endpoint.clone(),
            process: None,
        });
        let local = endpoint.address()?;
        let request = process::request(
            &self.key.config,
            (&self.key.host, self.key.port),
            local,
            false,
        );
        let original = resources
            .prepare_owned_process_with_endpoint(request, &tasks, &endpoint)
            .map_err(io::Error::other)?;
        self.pending_endpoint
            .lock()
            .unwrap()
            .as_mut()
            .expect("same actor owns opening")
            .process = Some(original.clone());
        let running = Arc::new(PluginLease {
            child: Mutex::new(PluginChild::Owned(original.clone())),
            endpoint: local,
            readiness: OnceLock::new(),
            process_endpoint: Some(endpoint.clone()),
        });
        *slot = Some(running.clone());
        let previous = self.pending_endpoint.lock().unwrap().take();
        drop(previous);
        let mut attempt = Attempt(running.clone(), true);
        let reservations = endpoint
            .close_reservations(end)
            .await
            .map_err(io::Error::other)?;
        if !reservations.matches(&endpoint) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "foreign reservation closure",
            ));
        }
        original.start(actor.clone())?;
        original.wait_started(end).await?;
        let readiness = self.ready_endpoint(&running, &endpoint, &actor, end);
        let result = tokio::select! {biased;
            _=self.cancel.cancelled()=>Err(revoked()),
            result=original.wait_root_exit()=>result.and_then(|_|Err(io::Error::other("plugin exited during readiness"))),
            result=readiness=>result,
        };
        let succeeded = result.is_ok();
        let _ = running.readiness.set(result.map_err(Arc::new));
        attempt.1 = !succeeded;
        running.ready()?;
        Ok(Some(running))
    }
    async fn ready_endpoint(
        &self,
        running: &PluginLease,
        endpoint: &Arc<ProcessEndpointHandoff>,
        actor: &TaskJoinWitness,
        end: Instant,
    ) -> io::Result<()> {
        // Preserve the original local startup bound and the caller's tighter end.
        let end = end.min(Instant::now() + Duration::from_secs(10));
        loop {
            running.check()?;
            if !self.supports_tcp() {
                return Ok(());
            }
            let attempt = endpoint.open_probe(actor, end);
            tokio::pin!(attempt);
            let result = loop {
                tokio::select! {biased;
                    _=self.cancel.cancelled()=>return Err(revoked()),
                    result=&mut attempt=>break result,
                    _=tokio::time::sleep(Duration::from_millis(10))=>running.check()?,
                }
            };
            match result {
                Ok(mut probe) => {
                    let endpoints = (probe.local_addr()?, probe.peer_addr()?);
                    let correct = endpoints.0 != endpoints.1 && endpoints.1 == running.endpoint;
                    let closed = probe
                        .close(end)
                        .await
                        .map_err(io::Error::other)?
                        .filter(|receipt| receipt.matches(&probe))
                        .ok_or_else(|| {
                            io::Error::new(
                                io::ErrorKind::TimedOut,
                                "actual readiness probe remains unclosed",
                            )
                        })?;
                    if !closed.had_native_acquisition() {
                        return Err(io::Error::other(
                            "successful probe lacks its original native acquisition",
                        ));
                    }
                    if !correct {
                        return Err(io::Error::new(
                            io::ErrorKind::PermissionDenied,
                            "actual readiness probe endpoint mismatch",
                        ));
                    }
                    running.check()?;
                    return Ok(());
                }
                Err(source) => {
                    // Failed connection's exact original must be explicitly
                    // closed before retry; Unknown is retained, never bypassed.
                    if endpoint
                        .close_probe(end)
                        .await
                        .map_err(io::Error::other)?
                        .is_none()
                    {
                        return Err(io::Error::other(source));
                    }
                    running.check()?;
                    if Instant::now() >= end {
                        return Err(io::Error::new(io::ErrorKind::TimedOut, source));
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

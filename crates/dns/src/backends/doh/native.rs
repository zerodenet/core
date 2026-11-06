//! The existing DoH cache uses one original host driver, never a second pool.
use super::*;
use crate::native::{Attempt, NativeDnsProvider};
use std::sync::{
    Mutex, OnceLock,
    atomic::{AtomicBool, Ordering},
};
use tokio::time::Instant;
use zero_platform_tokio::{contract::scope::TaskJoinWitness, controlled_carrier::loan::LoanOwner};

pub(super) struct NativeClient {
    pub(super) task: OnceLock<TaskJoinWitness>,
    pub(super) original: tokio::sync::Mutex<Option<LoanOwner>>,
    provider: Mutex<Option<Arc<dyn NativeDnsProvider>>>,
    closed: AtomicBool,
    // Read-only facts copied only from this actual canonical provider/loan.
    host_instance: Option<zero_core::component::ComponentInstanceId>,
    origin: OnceLock<zero_platform_tokio::controlled_carrier::CarrierOrigin>,
}
impl NativeClient {
    pub(in super::super) fn retained_instances(
        &self,
    ) -> io::Result<Vec<zero_core::component::ComponentInstanceId>> {
        if self.retired() {
            return Ok(Vec::new());
        }
        let instance = self
            .host_instance
            .ok_or_else(|| io::Error::other("actual DoH host Scope unavailable"))?;
        let mut result = std::collections::BTreeSet::from([instance]);
        let mut parent = self.origin.get();
        while let Some(origin) = parent {
            result.insert(origin.instance());
            parent = origin.parent();
        }
        Ok(result.into_iter().collect())
    }
    pub(super) fn retired(&self) -> bool {
        self.closed()
            && self.task.get().is_some_and(|task| {
                task.try_joined().is_some_and(|joined| {
                    task.matches_joined(&joined) && joined.registration_released()
                })
            })
    }
    pub(in super::super) fn record_original(&self, owner: &LoanOwner) -> io::Result<()> {
        self.origin
            .set(owner.original().clone())
            .map_err(|_| io::Error::other("actual DoH loan origin already installed"))
    }
    pub(super) fn new(provider: Arc<dyn NativeDnsProvider>) -> Self {
        let host_instance = provider.host_scope().map(|(instance, _)| instance);
        Self {
            host_instance,
            origin: OnceLock::new(),
            task: OnceLock::new(),
            original: Default::default(),
            provider: Mutex::new(Some(provider)),
            closed: AtomicBool::new(false),
        }
    }
    pub(super) fn retains_live_original(&self) -> bool {
        let Some(task) = self.task.get() else {
            return false;
        };
        if task.is_joined() || self.closed() {
            return false;
        }
        let provider = self
            .provider
            .lock()
            .expect("DoH provider owner poisoned")
            .clone();
        let Some(provider) = provider else {
            return false;
        };
        if !provider
            .tasks()
            .observe()
            .iter()
            .any(|registered| registered.task.same_registration(task))
        {
            return false;
        }
        self.original.try_lock().is_ok_and(|original| {
            original
                .as_ref()
                .is_some_and(|original| original.original().closed().is_none())
        })
    }
    pub(super) fn closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
    pub(super) async fn settle(&self, end: Instant) -> io::Result<()> {
        if self.closed() {
            return Ok(());
        }
        let task = self
            .task
            .get()
            .ok_or_else(|| io::Error::other("DoH driver registration is not bound"))?;
        let joined = tokio::time::timeout_at(end, task.wait())
            .await
            .map_err(|source| io::Error::new(io::ErrorKind::TimedOut, source))?;
        let mut original = tokio::time::timeout_at(end, self.original.lock())
            .await
            .map_err(|source| io::Error::new(io::ErrorKind::TimedOut, source))?;
        if let Some(owner) = original.as_mut() {
            let report = owner.close(end).await.map_err(io::Error::other)?;
            if !report.is_complete() || !owner.closed().is_some_and(|closed| closed.matches(owner))
            {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "DoH original loan custody is not returned",
                ));
            }
        }
        let provider = self
            .provider
            .lock()
            .expect("DoH provider owner poisoned")
            .clone()
            .ok_or_else(|| {
                io::Error::other("DoH original provider was lost before confirmation")
            })?;
        provider.close_stream_originals(task.clone(), end).await?;
        // Drop the actual controls/provider only after all confirmations. Old
        // cache/receipt handles then retain no original I/O or task owner.
        original.take();
        self.provider
            .lock()
            .expect("DoH provider owner poisoned")
            .take();
        self.closed.store(true, Ordering::Release);
        joined.result().map_err(io::Error::other)
    }
}
/// Logical exchange completion or confirmed retirement of its actual pool
/// owners. This is deliberately not a native-close receipt.
pub(crate) struct NativeCompletion(());
impl NativeCompletion {
    pub(crate) fn is_settled(&self) -> bool {
        true
    }
}
impl DohDnsResolver {
    pub(crate) async fn exchange_native(
        &self,
        provider: Arc<dyn NativeDnsProvider>,
        detour: Option<&str>,
        query: &[u8],
        end: Instant,
    ) -> Attempt {
        self.exchange_native_with_peers(provider, detour, query, end, None)
            .await
    }
    pub(crate) async fn exchange_native_at(
        &self,
        provider: Arc<dyn NativeDnsProvider>,
        query: &[u8],
        end: Instant,
        peers: &[SocketAddr],
    ) -> Attempt {
        self.exchange_native_with_peers(provider, None, query, end, Some(peers))
            .await
    }
    async fn exchange_native_with_peers(
        &self,
        provider: Arc<dyn NativeDnsProvider>,
        detour: Option<&str>,
        query: &[u8],
        end: Instant,
        peers: Option<&[SocketAddr]>,
    ) -> Attempt {
        let provider = match self.owner.select_host(provider) {
            Ok(provider) => provider,
            Err(source) => return Attempt::blocked(source),
        };
        let mut last = None;
        let addresses = peers
            .map(<[SocketAddr]>::to_vec)
            .unwrap_or_else(|| self.endpoint_addresses());
        for addr in addresses {
            let result = tokio::time::timeout_at(end, async {
                let mut client = self
                    .ready_native_client(addr, detour, provider.clone(), end, peers)
                    .await?;
                let request = self.request(query.len())?;
                let (response, body) = client
                    .sender
                    .send_request(request, false)
                    .map_err(io::Error::other)?;
                let mut request = NativeRequest {
                    body,
                    complete: false,
                };
                request
                    .body
                    .send_data(Bytes::copy_from_slice(query), true)
                    .map_err(io::Error::other)?;
                let answer = read_response(response.await.map_err(io::Error::other)?).await?;
                crate::message::parse_response(query, &answer)?;
                client.complete()?;
                request.complete = true;
                // The same actual lease is dropped here before evidence of
                // logical completion can be handed to the retry policy.
                drop(request);
                drop(client);
                Ok(answer)
            })
            .await
            .map_err(|source| io::Error::new(io::ErrorKind::TimedOut, source))
            .and_then(std::convert::identity);
            match result {
                Ok(answer) => return Attempt::pooled(Ok(answer), Some(NativeCompletion(()))),
                Err(opening) => {
                    if let Err(cleanup) = self.owner.drain_retired(end).await {
                        return Attempt::cleanup_failed(opening, cleanup);
                    }
                    last = Some(opening);
                    if Instant::now() >= end {
                        break;
                    }
                }
            }
        }
        Attempt::pooled(
            Err(last.unwrap_or_else(|| io::Error::other("DoH has no original endpoint"))),
            Some(NativeCompletion(())),
        )
    }
}

// A cancelled logical request resets only its actual H2 stream. The host's
// independent driver and carrier owner remain available to other/future users.
struct NativeRequest {
    body: h2::SendStream<Bytes>,
    complete: bool,
}
impl Drop for NativeRequest {
    fn drop(&mut self) {
        if !self.complete {
            self.body.send_reset(h2::Reason::CANCEL);
        }
    }
}

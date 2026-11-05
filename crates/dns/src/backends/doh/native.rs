//! The existing DoH cache uses one original host driver, never a second pool.
use super::*;
use crate::native::{Attempt, NativeDnsProvider};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Mutex, OnceLock,
};
use tokio::time::Instant;
use zero_platform_tokio::{contract::scope::TaskJoinWitness, controlled_carrier::loan::LoanOwner};

pub(super) struct NativeClient {
    pub(super) task: OnceLock<TaskJoinWitness>,
    pub(super) original: tokio::sync::Mutex<Option<LoanOwner>>,
    provider: Mutex<Option<Arc<dyn NativeDnsProvider>>>,
    closed: AtomicBool,
}
impl NativeClient {
    pub(super) fn new(provider: Arc<dyn NativeDnsProvider>) -> Self {
        Self {
            task: OnceLock::new(),
            original: Default::default(),
            provider: Mutex::new(Some(provider)),
            closed: AtomicBool::new(false),
        }
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
        if let Err(source) = self.owner.select_host(provider.tasks()) {
            return Attempt::blocked(source);
        }
        let mut last = None;
        for addr in self.endpoint_addresses() {
            let result = tokio::time::timeout_at(end, async {
                let mut client = self
                    .ready_native_client(addr, detour, provider.clone(), end)
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

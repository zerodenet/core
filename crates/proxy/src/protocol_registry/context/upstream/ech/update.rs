//! The existing cache entry retains the one actual Node Query, never a task wrapper.
use super::*;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    OnceLock,
};
use tokio::sync::Notify;

pub(super) struct Update {
    original: OnceLock<zero_dns::EchRefresh>,
    initialized: AtomicBool,
    result: Mutex<Option<Result<CacheRecord, Arc<io::Error>>>>,
    changed: Notify,
    waiters: AtomicUsize,
    background: AtomicBool,
}
impl Update {
    fn new(background: bool) -> Self {
        Self {
            original: OnceLock::new(),
            initialized: AtomicBool::new(false),
            result: Mutex::default(),
            changed: Notify::new(),
            waiters: AtomicUsize::new(0),
            background: AtomicBool::new(background),
        }
    }
    fn pending(&self) -> bool {
        if !self.initialized.load(Ordering::Acquire){return true}
        self.original.get().map_or_else(
            || self.result.lock().expect("ECH result poisoned").is_none(),
            |original| !original.task.is_joined(),
        )
    }
    fn publish(&self, result: Result<CacheRecord, Arc<io::Error>>) {
        *self.result.lock().expect("ECH result poisoned") = Some(result);
        self.changed.notify_waiters();
    }
    pub(super) async fn wait(self: &Arc<Self>) -> io::Result<CacheRecord> {
        self.waiters.fetch_add(1, Ordering::AcqRel);
        let _waiting = Waiting(self.clone());
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let result = self.result.lock().expect("ECH result poisoned").clone();
            if let Some(result) = result.filter(|_|self.initialized.load(Ordering::Acquire)) {
                if let Some(original) = self.original.get() {
                    original
                        .task
                        .wait()
                        .await
                        .result()
                        .map_err(io::Error::other)?;
                }
                return result.map_err(retained);
            }
            if let Some(original) = self.original.get() {
                tokio::select! {
                    _ = &mut changed => {},
                    joined = original.task.wait() => {
                        joined.result().map_err(io::Error::other)?;
                        // A cancelled operation can have no callback result.
                        // Its actual Query join remains separate from native
                        // closure, which that same provider finalizer consumed.
                        if self.result.lock().expect("ECH result poisoned").is_none() {
                            return Err(io::Error::new(io::ErrorKind::Interrupted, "original ECH query retired before cache publication"));
                        }
                    }
                }
            } else {
                changed.await;
            }
        }
    }
}
struct Waiting(Arc<Update>);
impl Drop for Waiting {
    fn drop(&mut self) {
        if self.0.waiters.fetch_sub(1, Ordering::AcqRel) == 1
            && !self.0.background.load(Ordering::Acquire)
        {
            if let Some(original) = self.0.original.get() {
                original.cancel.cancel();
            }
        }
    }
}
impl RuntimeEchResolver {
    pub(super) fn start_update(
        &self,
        key: CacheKey,
        entry: Arc<Entry>,
        force: EchForceQuery,
        background: bool,
    ) -> Arc<Update> {
        let mut active = entry.update.lock().expect("ECH update poisoned");
        if let Some(update) = active.as_ref().filter(|update| {
            update.pending()
                || (!background
                    && update
                        .result
                        .lock()
                        .expect("ECH result poisoned")
                        .as_ref()
                        .is_some_and(|result| {
                            result.as_ref().is_ok_and(|record| {
                                record.failure.is_none() && record.expires > Instant::now()
                            })
                        }))
        }) {
            if background {
                update.background.store(true, Ordering::Release);
            }
            return update.clone();
        }
        let update = Arc::new(Update::new(background));
        let previous=active.replace(update.clone());
        drop(active);
        drop(previous);
        let original_entry = entry.clone();
        let completed = update.clone();
        let dns = self.dns.clone();
        let expected = zero_platform_tokio::contract::dns::DnsGeneration::new(
            key.resolver_generation,
            key.egress_generation,
        );
        let started =
            self.dns
                .spawn_ech_refresh(key.query_name.clone(), key.server.clone(), move |answer| {
                    Box::pin(async move {
                        let result = record(answer, force);
                        let mut cache = original_entry.record.write().await;
                        if dns.generation() != expected {
                            completed.publish(Err(Arc::new(io::Error::new(
                                io::ErrorKind::Interrupted,
                                "ECH cache update generation retired",
                            ))));
                            return;
                        }
                        if let Ok(record) = &result {
                            *cache = Some(record.clone());
                        }
                        completed.publish(result);
                    })
                });
        match started {
            Ok(original) => {
                update
                    .original
                    .set(original)
                    .expect("one actual ECH update registration");
            }
            Err(source) => {
                // Admission failed before any DNS/native job. Cache metadata
                // can retain that original typed failure without spawning IO.
                update.publish(Err(Arc::new(source)));
            }
        }
        update.initialized.store(true,Ordering::Release);
        update.changed.notify_waiters();
        update
    }
}
fn record(
    answer: io::Result<zero_dns::EchDnsRecord>,
    force: EchForceQuery,
) -> Result<CacheRecord, Arc<io::Error>> {
    match answer {
        Ok(answer) => Ok(CacheRecord {
            material: answer.config_list,
            expires: Instant::now()
                + if answer.ttl_seconds == 0 {
                    DEFAULT_TTL
                } else {
                    Duration::from_secs(u64::from(answer.ttl_seconds))
                },
            failure: None,
        }),
        Err(source) if force == EchForceQuery::Full => Err(Arc::new(source)),
        Err(source) => Ok(CacheRecord {
            material: None,
            expires: Instant::now() + DEFAULT_TTL,
            failure: Some(source.to_string()),
        }),
    }
}
#[derive(Debug)]
struct OriginalError(Arc<io::Error>);
impl std::fmt::Display for OriginalError {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self.0.as_ref(), fmt)
    }
}
impl std::error::Error for OriginalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
fn retained(source: Arc<io::Error>) -> io::Error {
    io::Error::new(source.kind(), OriginalError(source))
}

#[derive(Debug)]
struct QueryFailure(io::Error);
impl std::fmt::Display for QueryFailure {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(fmt, "ECH DNS query failed: {}", self.0)
    }
}
impl std::error::Error for QueryFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
pub(super) fn query_failure(source: io::Error) -> io::Error {
    io::Error::new(source.kind(), QueryFailure(source))
}

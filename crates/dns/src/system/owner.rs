//! The original blocking resolver job survives cancellation of its waiter.
use super::*;
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::Poll,
};
use tokio::{task::JoinHandle, time::Instant};

const MAX_LOOKUPS: usize = 16;

#[derive(Default, Debug)]
pub(super) struct LookupOwner {
    state: Mutex<State>,
}
#[derive(Default, Debug)]
struct State {
    admission: Admission,
    jobs: Vec<Arc<Job>>,
    failure: Option<Arc<io::Error>>,
}
#[derive(Default, Debug, PartialEq, Eq)]
enum Admission {
    #[default]
    Open,
    StagedClose,
    Revoked,
}
#[derive(Debug)]
pub(super) struct Job {
    original: Mutex<Option<JoinHandle<io::Result<Vec<IpAddress>>>>>,
    result: Mutex<Option<Result<Vec<IpAddress>, Arc<io::Error>>>>,
    driver: tokio::sync::Mutex<()>,
    panicked: std::sync::atomic::AtomicBool,
}
impl Job {
    pub(super) async fn join(&self) -> io::Result<Vec<IpAddress>> {
        let _driver = self.driver.lock().await;
        if let Some(result) = self
            .result
            .lock()
            .expect("system lookup result poisoned")
            .as_ref()
        {
            return result.clone().map_err(retained);
        }
        let result = std::future::poll_fn(|cx| {
            let mut original = self
                .original
                .lock()
                .expect("system lookup original poisoned");
            let task = original
                .as_mut()
                .expect("original installed before any await");
            match Pin::new(task).poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(result) => {
                    self.panicked
                        .store(result.is_err(), std::sync::atomic::Ordering::Release);
                    original.take();
                    Poll::Ready(
                        result
                            .map_err(io::Error::other)
                            .and_then(std::convert::identity),
                    )
                }
            }
        })
        .await
        .map_err(Arc::new);
        *self.result.lock().expect("system lookup result poisoned") = Some(result.clone());
        result.map_err(retained)
    }
    #[cfg(test)]
    pub(super) fn complete(&self) -> bool {
        self.result
            .lock()
            .expect("system lookup result poisoned")
            .is_some()
    }
}
impl LookupOwner {
    pub(super) fn start(
        &self,
        work: impl FnOnce() -> io::Result<Vec<IpAddress>> + Send + 'static,
    ) -> io::Result<Arc<Job>> {
        let mut state = self.state.lock().expect("system lookup owner poisoned");
        Self::collect_completed(&mut state);
        if let Some(source) = &state.failure {
            return Err(retained(source.clone()));
        }
        if state.admission != Admission::Open {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "system resolver is closing",
            ));
        }
        if state.jobs.len() >= MAX_LOOKUPS {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "system resolver capacity exhausted",
            ));
        }
        let job = Arc::new(Job {
            original: Mutex::new(None),
            result: Mutex::new(None),
            driver: tokio::sync::Mutex::new(()),
            panicked: Default::default(),
        });
        // Install actual custody before spawning. No await or cancellation
        // can occur between spawning and retaining that same JoinHandle.
        state.jobs.push(job.clone());
        *job.original
            .lock()
            .expect("system lookup original poisoned") = Some(tokio::task::spawn_blocking(work));
        Ok(job)
    }
    pub(super) async fn resolve(&self, domain: String) -> io::Result<Vec<IpAddress>> {
        use std::net::ToSocketAddrs;
        self.reap_finished().await;
        let job = self.start(move || {
            (domain.as_str(), 0).to_socket_addrs().map(|addresses| {
                addresses
                    .map(|address| ip_addr_to_ip(address.ip()))
                    .collect()
            })
        })?;
        let result = job.join().await;
        Self::collect_completed(&mut self.state.lock().expect("system lookup owner poisoned"));
        result
    }
    async fn reap_finished(&self) {
        let jobs = self
            .state
            .lock()
            .expect("system lookup owner poisoned")
            .jobs
            .clone();
        for job in jobs {
            let ready = job
                .original
                .lock()
                .expect("system lookup original poisoned")
                .as_ref()
                .is_some_and(JoinHandle::is_finished);
            if ready {
                if let Err(source) = job.join().await {
                    self.record_failure(&job, source);
                }
            }
        }
    }
    pub(super) fn revoke(&self) {
        self.state
            .lock()
            .expect("system lookup owner poisoned")
            .admission = Admission::Revoked;
    }
    pub(super) fn is_open(&self) -> bool {
        let state = self.state.lock().expect("system lookup owner poisoned");
        state.admission == Admission::Open && state.failure.is_none()
    }
    pub(super) fn retained(&self) -> bool {
        let state = self.state.lock().expect("system lookup owner poisoned");
        !state.jobs.is_empty() || state.failure.is_some()
    }
    fn collect_completed(state: &mut State) {
        state.jobs.retain(|job| {
            let result = job.result.lock().expect("system lookup result poisoned");
            if job.panicked.load(std::sync::atomic::Ordering::Acquire) {
                if let Some(Err(source)) = result.as_ref() {
                    state.failure.get_or_insert_with(|| source.clone());
                }
            }
            result.is_none()
        });
    }
    pub(super) fn close_idle(&self) -> bool {
        let mut state = self.state.lock().expect("system lookup owner poisoned");
        Self::collect_completed(&mut state);
        if !state.jobs.is_empty() || state.failure.is_some() {
            return false;
        }
        if state.admission == Admission::Open {
            state.admission = Admission::StagedClose;
        }
        true
    }
    pub(super) fn reopen_idle(&self) {
        let mut state = self.state.lock().expect("system lookup owner poisoned");
        if state.admission == Admission::StagedClose
            && state.jobs.is_empty()
            && state.failure.is_none()
        {
            state.admission = Admission::Open;
        }
    }
    pub(super) async fn shutdown(&self, end: Instant) -> io::Result<()> {
        self.revoke();
        self.drain(end).await
    }
    pub(super) async fn drain(&self, end: Instant) -> io::Result<()> {
        loop {
            let job = self
                .state
                .lock()
                .expect("system lookup owner poisoned")
                .jobs
                .first()
                .cloned();
            let Some(job) = job else {
                return self
                    .state
                    .lock()
                    .expect("system lookup owner poisoned")
                    .failure
                    .as_ref()
                    .map_or(Ok(()), |source| Err(retained(source.clone())));
            };
            // DNS errors are ordinary results. Awaiting this exact handle,
            // including an original panic, establishes only job completion.
            let result = tokio::time::timeout_at(end, job.join())
                .await
                .map_err(|source| io::Error::new(io::ErrorKind::TimedOut, source))?;
            if let Err(source) = result {
                self.record_failure(&job, source);
            }
            self.state
                .lock()
                .expect("system lookup owner poisoned")
                .jobs
                .retain(|original| !Arc::ptr_eq(original, &job));
        }
    }
    fn record_failure(&self, job: &Job, source: io::Error) {
        if job.panicked.load(std::sync::atomic::Ordering::Acquire) {
            self.state
                .lock()
                .expect("system lookup owner poisoned")
                .failure
                .get_or_insert_with(|| Arc::new(source));
        }
    }
}
#[derive(Debug)]
struct Retained(Arc<io::Error>);
impl std::fmt::Display for Retained {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for Retained {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
fn retained(source: Arc<io::Error>) -> io::Error {
    io::Error::new(source.kind(), Retained(source))
}

//! Bounded target behavior probes used to reproduce observable REALITY TLS output.
mod ccs;
mod io;

use self::{ccs::accepts_ccs, io::observe_post_handshake};
use super::Profile;
use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{watch, OnceCell, Semaphore};
use zero_transport::handshake_target::Connector;

const MAX_PROBE_KEYS: usize = 256;
const MAX_CONCURRENT_PROBES: usize = 8;
const PROBE_TIMEOUT: Duration = Duration::from_secs(8);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AlpnClass {
    None,
    Http1,
    H2,
}

impl AlpnClass {
    pub(super) fn from_offered(offered: &[String]) -> Self {
        match offered.first().map(String::as_str) {
            None => Self::None,
            Some("h2") => Self::H2,
            Some(_) => Self::Http1,
        }
    }

    fn protocols(self) -> Vec<String> {
        match self {
            Self::None => Vec::new(),
            Self::Http1 => vec!["http/1.1".to_owned()],
            Self::H2 => vec!["h2".to_owned(), "http/1.1".to_owned()],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Detection {
    pub(super) post_handshake_lengths: Vec<usize>,
    pub(super) max_ccs_records: usize,
}

impl Default for Detection {
    fn default() -> Self {
        Self {
            post_handshake_lengths: Vec::new(),
            max_ccs_records: usize::MAX,
        }
    }
}

struct Entry {
    value: OnceCell<Detection>,
    cancelled: watch::Sender<bool>,
}

impl Entry {
    fn new() -> Self {
        let (cancelled, _) = watch::channel(false);
        Self {
            value: OnceCell::new(),
            cancelled,
        }
    }
}

struct State {
    entries: Arc<Mutex<HashMap<String, Arc<Entry>>>>,
    permits: Arc<Semaphore>,
    generation: AtomicU64,
}

impl Default for State {
    fn default() -> Self {
        Self {
            entries: Arc::new(Mutex::new(HashMap::new())),
            permits: Arc::new(Semaphore::new(MAX_CONCURRENT_PROBES)),
            generation: AtomicU64::new(0),
        }
    }
}

impl State {
    fn retire(&self) {
        self.generation.fetch_add(1, Ordering::SeqCst);
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        for (_, entry) in entries.drain() {
            entry.cancelled.send_replace(true);
        }
    }
}

struct Owner {
    state: Arc<State>,
}

impl Drop for Owner {
    fn drop(&mut self) {
        self.state.retire();
    }
}

#[derive(Clone)]
pub(crate) struct Registry(Arc<Owner>);

impl Default for Registry {
    fn default() -> Self {
        Self(Arc::new(Owner {
            state: Arc::new(State::default()),
        }))
    }
}

#[derive(Clone)]
pub(crate) struct Access {
    state: Arc<State>,
    generation: u64,
}

impl Default for Access {
    fn default() -> Self {
        let registry = Registry::default();
        let access = registry.access();
        drop(registry);
        access
    }
}

impl std::fmt::Debug for Registry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RealityTargetProbeRegistry")
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for Access {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RealityTargetProbeAccess")
            .finish_non_exhaustive()
    }
}

impl Registry {
    pub(crate) fn access(&self) -> Access {
        Access {
            generation: self.0.state.generation.load(Ordering::SeqCst),
            state: self.0.state.clone(),
        }
    }

    pub(crate) fn retire(&self) {
        self.0.state.retire();
    }

    #[cfg(test)]
    async fn detect_cached<F, Fut>(&self, key: String, probe: F) -> Detection
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Detection>,
    {
        self.access().detect_cached(key, probe).await
    }
}

impl Access {
    pub(crate) async fn detect(
        &self,
        profile: &Profile,
        connector: &Connector,
        server_name: &str,
        alpn: AlpnClass,
    ) -> Detection {
        let key = format!(
            "{:?}:{}:{server_name}:{alpn:?}",
            profile.endpoint, profile.proxy_protocol
        );
        self.detect_cached(key, || async {
            tokio::time::timeout(PROBE_TIMEOUT, probe(profile, connector, server_name, alpn))
                .await
                .unwrap_or_default()
        })
        .await
    }

    async fn detect_cached<F, Fut>(&self, key: String, probe: F) -> Detection
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Detection>,
    {
        if self.generation != self.state.generation.load(Ordering::SeqCst) {
            return Detection::default();
        }
        let entry = {
            let mut entries = self
                .state
                .entries
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if self.generation != self.state.generation.load(Ordering::SeqCst) {
                return Detection::default();
            }
            if let Some(entry) = entries.get(&key) {
                entry.clone()
            } else {
                if entries.len() >= MAX_PROBE_KEYS {
                    return Detection::default();
                }
                let entry = Arc::new(Entry::new());
                entries.insert(key, entry.clone());
                entry
            }
        };
        let mut cancelled = entry.cancelled.subscribe();
        let permits = self.state.permits.clone();
        entry
            .value
            .get_or_init(|| async {
                tokio::select! {
                    result = async {
                        let Ok(_permit) = permits.acquire_owned().await else {
                            return Detection::default();
                        };
                        probe().await
                    } => result,
                    _ = async {
                        if !*cancelled.borrow() {
                            let _ = cancelled.changed().await;
                        }
                    } => Detection::default(),
                }
            })
            .await
            .clone()
    }
}

async fn probe(
    profile: &Profile,
    connector: &Connector,
    server_name: &str,
    alpn: AlpnClass,
) -> Detection {
    let post = observe_post_handshake(profile, connector, server_name, alpn);
    let ccs = accepts_ccs(profile, connector, server_name, alpn);
    let (post, ccs) = tokio::join!(post, ccs);
    Detection {
        post_handshake_lengths: post.unwrap_or_default(),
        max_ccs_records: ccs.unwrap_or(usize::MAX),
    }
}

fn client_config(server_name: &str, alpn: AlpnClass) -> ztls::handshake::Tls13Config {
    ztls::handshake::Tls13Config {
        server_name: server_name.to_owned(),
        alpn_protocols: alpn.protocols(),
        handshake_timeout_ms: PROBE_TIMEOUT.as_millis() as u64,
        ..Default::default()
    }
}

#[cfg(test)]
#[path = "../../../tests/reality/target_probe.rs"]
mod tests;

//! Initial URL rules use retained original network and cache owners.
use crate::application::services::network::{
    download::DownloadEvent, ApplicationEventSinkTcpDialer,
};
use std::{
    error::Error,
    fs, io,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use zero_config::{RuleSetSourceType, RuntimeConfig};
use zero_runtime::configuration::{ConfigurationSource, RuleSetCacheDownload};

struct BootstrapAttempt {
    source: Arc<ConfigurationSource>,
    network: ApplicationEventSinkTcpDialer,
    network_started: Mutex<bool>,
    cache: Mutex<Option<RuleSetCacheDownload>>,
}
impl BootstrapAttempt {
    async fn retire_network(&self) -> io::Result<()> {
        let started = *self.network_started.lock().unwrap();
        if started {
            self.network
                .retire_generation(tokio::time::Instant::now() + Duration::from_secs(10))
                .await?;
            *self.network_started.lock().unwrap() = false;
        }
        Ok(())
    }
    async fn cleanup(&self) -> io::Result<()> {
        let network = self.retire_network().await.err();
        // Uncertain network retirement never skips the independent file close.
        let cache = self.cache.lock().unwrap().as_ref().cloned();
        let cache_error = match cache {
            Some(cache) => cache.abort().err(),
            None => self.source.abort_rule_set_cache().err(),
        };
        if cache_error.is_none() {
            self.cache.lock().unwrap().take();
        }
        if network.is_some() || cache_error.is_some() {
            return Err(io::Error::other(CleanupFailure {
                network,
                cache: cache_error,
            }));
        }
        self.source.confirm_original_io_closed()
    }
}
#[derive(Debug)]
struct CleanupFailure {
    network: Option<io::Error>,
    cache: Option<io::Error>,
}
impl std::fmt::Display for CleanupFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "rule bootstrap cleanup unconfirmed: network={:?}; cache={:?}",
            self.network, self.cache
        )
    }
}
impl Error for CleanupFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.network
            .as_ref()
            .or(self.cache.as_ref())
            .map(|error| error as &dyn Error)
    }
}
struct BootstrapSlot {
    accepted: tokio::sync::Mutex<()>,
    original: Mutex<Option<Arc<BootstrapAttempt>>>,
}
impl BootstrapSlot {
    const fn new() -> Self {
        Self {
            accepted: tokio::sync::Mutex::const_new(()),
            original: Mutex::new(None),
        }
    }
    async fn load(
        &self,
        source: Arc<ConfigurationSource>,
        path: &str,
    ) -> Result<RuntimeConfig, Box<dyn Error>> {
        let _accepted = self.accepted.lock().await;
        let previous = self.original.lock().unwrap().clone();
        if let Some(previous) = previous {
            // No document, socket, task or cache replacement is constructed
            // before the interrupted attempt's actual cleanup confirms.
            previous.cleanup().await?;
            self.original.lock().unwrap().take();
        }
        let original = Arc::new(BootstrapAttempt {
            source: source.clone(),
            network: ApplicationEventSinkTcpDialer::new(
                zero_platform_tokio::EgressInterfaceControl::default(),
            ),
            network_started: Mutex::new(false),
            cache: Mutex::new(None),
        });
        *self.original.lock().unwrap() = Some(original.clone());
        let mut config = source.load_initial_configuration(path)?;
        let source_dir = config.source_dir.clone();
        for rule_set in &mut config.route.rule_sets {
            if rule_set.source_type != RuleSetSourceType::Url {
                continue;
            }
            let Some(url) = rule_set.url.as_deref() else {
                tracing::warn!(tag=%rule_set.tag, "url rule set missing `url` field");
                continue;
            };
            let cache_path = resolve_path(&rule_set.path, source_dir.as_deref());
            if is_cache_fresh(&cache_path, rule_set.update_interval_seconds) {
                rule_set.path = cache_path.to_string_lossy().into_owned();
                continue;
            }
            match fetch(&original, url, &cache_path).await {
                Ok(bytes) => {
                    tracing::info!(tag=%rule_set.tag, path=%cache_path.display(), bytes, "rule set fetched and cached");
                    rule_set.path = cache_path.to_string_lossy().into_owned();
                }
                Err(error) => {
                    // Ordinary HTTP/parse/write errors may use stale data;
                    // uncertain cleanup cannot authorize that handoff.
                    original.cleanup().await?;
                    if cache_path.exists() {
                        tracing::warn!(tag=%rule_set.tag, error=%error, "rule set fetch failed; using stale cache");
                        rule_set.path = cache_path.to_string_lossy().into_owned();
                    } else {
                        tracing::error!(tag=%rule_set.tag, error=%error, "rule set fetch failed; no cache available");
                    }
                }
            }
        }
        original.cleanup().await?;
        self.original.lock().unwrap().take();
        Ok(config)
    }
}
static STARTUP: BootstrapSlot = BootstrapSlot::new();

pub(crate) async fn load_initial_configuration(
    source: Arc<ConfigurationSource>,
    path: &str,
) -> Result<RuntimeConfig, Box<dyn Error>> {
    STARTUP.load(source, path).await
}

async fn fetch(original: &BootstrapAttempt, url: &str, path: &Path) -> io::Result<u64> {
    let mut url = url::Url::parse(url).map_err(io::Error::other)?;
    let cache = original.source.prepare_rule_set_cache(path)?;
    *original.cache.lock().unwrap() = Some(cache.clone());
    for redirects in 0..5 {
        original.network.begin_generation()?;
        *original.network_started.lock().unwrap() = true;
        let mut download = original.network.download(url)?;
        let result = async {
            while let Some(event) = download.next().await? {
                match event {
                    DownloadEvent::Data(bytes) => cache.write_chunk(&bytes)?,
                    DownloadEvent::Redirect(url) => return Ok(Some(url)),
                }
            }
            Ok::<_, io::Error>(None)
        }
        .await;
        drop(download);
        // Actual HTTP/TLS/connect joins and returned views precede each native
        // controller receipt. Cancellation or a port probe is never a receipt.
        original.retire_network().await?;
        match result? {
            Some(next) if redirects < 4 => url = next,
            Some(_) => return Err(io::Error::other("reached max redirects (5)")),
            None => {
                let bytes = cache.finish()?;
                original.cache.lock().unwrap().take();
                return Ok(bytes);
            }
        }
    }
    unreachable!("bounded redirect loop returns on its final exchange")
}

fn is_cache_fresh(cache_path: &Path, interval: u64) -> bool {
    let Ok(modified) = fs::metadata(cache_path).and_then(|meta| meta.modified()) else {
        return false;
    };
    let age = modified
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    now.saturating_sub(age) < interval
}
fn resolve_path(path: &str, base_dir: Option<&Path>) -> PathBuf {
    let path = Path::new(path);
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base_dir.map_or_else(|| path.to_path_buf(), |base| base.join(path))
    }
}

#[cfg(test)]
#[path = "rule_set_fetch/tests.rs"]
mod tests;

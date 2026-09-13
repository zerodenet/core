//! Bounded TLS session context reuse, isolated by complete verification policy and CA bytes.
use std::{
    collections::VecDeque,
    path::PathBuf,
    sync::{Arc, LazyLock, Mutex},
};
static CONFIGS: LazyLock<Mutex<VecDeque<(String, rustls::ClientConfig)>>> =
    LazyLock::new(|| Mutex::new(VecDeque::new()));
pub(super) fn get(key: &str) -> Option<rustls::ClientConfig> {
    let mut configs = CONFIGS.lock().unwrap_or_else(|e| e.into_inner());
    let index = configs.iter().position(|(candidate, _)| candidate == key)?;
    let entry = configs.remove(index)?;
    let config = entry.1.clone();
    configs.push_back(entry);
    Some(config)
}
pub(super) fn insert(key: String, config: rustls::ClientConfig) -> rustls::ClientConfig {
    let mut configs = CONFIGS.lock().unwrap_or_else(|e| e.into_inner());
    if let Some((_, existing)) = configs.iter().find(|(candidate, _)| candidate == &key) {
        return existing.clone();
    }
    while configs.len() >= 128 {
        configs.pop_front();
    }
    configs.push_back((key, config.clone()));
    config
}

#[derive(Clone)]
pub(super) struct OpenSslClientConfig {
    pub(super) context: openssl::ssl::SslContext,
    pub(super) session: Arc<Mutex<Option<openssl::ssl::SslSession>>>,
}

#[derive(Clone, PartialEq, Eq)]
pub(super) struct OpenSslClientCacheKey {
    profile: crate::profile::OwnedClientTlsProfile,
    base_dir: Option<PathBuf>,
    effective_server_name: String,
    ca_digest: [u8; 32],
}

impl OpenSslClientCacheKey {
    pub(super) fn new(
        profile: crate::profile::OwnedClientTlsProfile,
        base_dir: Option<PathBuf>,
        effective_server_name: String,
        ca_digest: [u8; 32],
    ) -> Self {
        Self {
            profile,
            base_dir,
            effective_server_name,
            ca_digest,
        }
    }
}

static OPENSSL_CONFIGS: LazyLock<Mutex<VecDeque<(OpenSslClientCacheKey, OpenSslClientConfig)>>> =
    LazyLock::new(|| Mutex::new(VecDeque::new()));

pub(super) fn get_openssl(key: &OpenSslClientCacheKey) -> Option<OpenSslClientConfig> {
    let mut configs = OPENSSL_CONFIGS.lock().unwrap_or_else(|e| e.into_inner());
    let index = configs.iter().position(|(candidate, _)| candidate == key)?;
    let entry = configs.remove(index)?;
    let config = entry.1.clone();
    configs.push_back(entry);
    Some(config)
}

pub(super) fn insert_openssl(
    key: OpenSslClientCacheKey,
    config: OpenSslClientConfig,
) -> OpenSslClientConfig {
    let mut configs = OPENSSL_CONFIGS.lock().unwrap_or_else(|e| e.into_inner());
    if let Some((_, existing)) = configs.iter().find(|(candidate, _)| candidate == &key) {
        return existing.clone();
    }
    while configs.len() >= 128 {
        configs.pop_front();
    }
    configs.push_back((key, config.clone()));
    config
}

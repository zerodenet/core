//! Fetch URL-based rule sets, cache them locally, and update the config
//! paths to point at the cache.

use std::fs;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use zero_config::{RouteRuleSetConfig, RuleSetSourceType};

const MAX_DOWNLOADED_RULE_SET_BYTES: u64 = 1024 * 1024 * 1024;

/// For each URL-based rule set, fetch and cache to `path`.  If the cache
/// file exists and is newer than `update_interval_seconds`, skip the
/// fetch.  If the fetch fails and a cache file exists, use the stale
/// cache.
///
/// Relative paths are resolved against `base_dir`.
pub fn pre_fetch_rule_sets(rule_sets: &mut [RouteRuleSetConfig], base_dir: Option<&Path>) {
    for rule_set in rule_sets.iter_mut() {
        if rule_set.source_type != RuleSetSourceType::Url {
            continue;
        }
        let Some(ref url) = rule_set.url else {
            tracing::warn!(tag = %rule_set.tag, "url rule set missing `url` field");
            continue;
        };

        let cache_path = resolve_path(&rule_set.path, base_dir);

        // Check if cache is still fresh.
        if is_cache_fresh(&cache_path, rule_set.update_interval_seconds) {
            tracing::debug!(tag = %rule_set.tag, url = %url, "rule set cache is fresh");
            rule_set.path = cache_path.to_string_lossy().to_string();
            continue;
        }

        match ureq::get(url).call() {
            Ok(response) => {
                let downloaded = download_to_cache(response, &cache_path);
                let bytes = match downloaded {
                    Ok(bytes) => bytes,
                    Err(e) => {
                        tracing::warn!(
                            tag = %rule_set.tag, url = %url,
                            path = %cache_path.display(), error = %e,
                            "failed to install rule set cache"
                        );
                        continue;
                    }
                };
                if !cache_path.exists() {
                    tracing::warn!(
                        tag = %rule_set.tag, url = %url,
                        path = %cache_path.display(),
                        "rule set cache installation did not produce a file"
                    );
                    continue;
                }
                tracing::info!(
                    tag = %rule_set.tag, url = %url,
                    path = %cache_path.display(), bytes,
                    "rule set fetched and cached"
                );
                rule_set.path = cache_path.to_string_lossy().to_string();
            }
            Err(e) => {
                if cache_path.exists() {
                    tracing::warn!(
                        tag = %rule_set.tag, url = %url, error = %e,
                        "rule set fetch failed; using stale cache"
                    );
                    rule_set.path = cache_path.to_string_lossy().to_string();
                } else {
                    tracing::error!(
                        tag = %rule_set.tag, url = %url, error = %e,
                        "rule set fetch failed; no cache available"
                    );
                }
            }
        }
    }
}

fn download_to_cache(response: ureq::Response, cache_path: &Path) -> io::Result<u64> {
    if let Some(parent) = cache_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let temporary_path = sibling_temporary_path(cache_path, "download");
    let result = (|| {
        let mut temporary = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary_path)?;
        let mut reader = response
            .into_reader()
            .take(MAX_DOWNLOADED_RULE_SET_BYTES + 1);
        let bytes = io::copy(&mut reader, &mut temporary)?;
        if bytes > MAX_DOWNLOADED_RULE_SET_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "downloaded rule set exceeds {} bytes",
                    MAX_DOWNLOADED_RULE_SET_BYTES
                ),
            ));
        }
        temporary.flush()?;
        temporary.sync_all()?;
        drop(temporary);
        install_cache_file(&temporary_path, cache_path)?;
        Ok(bytes)
    })();

    if result.is_err() {
        let _ = fs::remove_file(&temporary_path);
    }
    result
}

fn install_cache_file(temporary_path: &Path, cache_path: &Path) -> io::Result<()> {
    if !cache_path.exists() {
        return fs::rename(temporary_path, cache_path);
    }

    // Never truncate or overwrite a potentially mapped ZRS file. Move the old
    // name aside first, install the new immutable file, then remove the backup.
    let backup_path = sibling_temporary_path(cache_path, "previous");
    fs::rename(cache_path, &backup_path)?;
    if let Err(error) = fs::rename(temporary_path, cache_path) {
        let _ = fs::rename(&backup_path, cache_path);
        return Err(error);
    }
    let _ = fs::remove_file(backup_path);
    Ok(())
}

fn sibling_temporary_path(path: &Path, role: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("rule-set");
    path.with_file_name(format!(
        ".{file_name}.{role}.{}.{}",
        std::process::id(),
        nonce
    ))
}

fn is_cache_fresh(cache_path: &Path, update_interval_seconds: u64) -> bool {
    let Ok(meta) = fs::metadata(cache_path) else {
        return false;
    };
    let Ok(modified) = meta.modified() else {
        return false;
    };
    let age_secs = modified
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    now - age_secs < update_interval_seconds
}

fn resolve_path(path: &str, base_dir: Option<&Path>) -> PathBuf {
    let candidate = Path::new(path);
    if candidate.is_absolute() {
        return candidate.to_path_buf();
    }
    match base_dir {
        Some(base_dir) => base_dir.join(candidate),
        None => candidate.to_path_buf(),
    }
}

use std::path::{Path, PathBuf};

/// Resolve the default persistent Fake-IP journal for a configuration source.
///
/// The source-directory hash keeps independent client installations separate.
/// `ZERO_DNS_STATE_DIR` overrides only the containing directory, which is
/// useful for service managers and isolated integration tests.
pub fn default_fake_ip_state_path(source_dir: &Path) -> PathBuf {
    let root = if let Some(path) = std::env::var_os("ZERO_DNS_STATE_DIR") {
        PathBuf::from(path)
    } else {
        default_state_root()
    };
    let canonical = source_dir
        .canonicalize()
        .unwrap_or_else(|_| source_dir.to_path_buf());
    let identity = canonical.to_string_lossy().into_owned();
    #[cfg(windows)]
    let identity = identity.to_ascii_lowercase();
    root.join(format!("fake-ip-{:016x}.jsonl", fnv1a(identity.as_bytes())))
}

fn default_state_root() -> PathBuf {
    #[cfg(windows)]
    {
        std::env::var_os("LOCALAPPDATA")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir)
            .join("Zero")
            .join("state")
    }
    #[cfg(unix)]
    {
        if let Some(path) = std::env::var_os("XDG_STATE_HOME") {
            return PathBuf::from(path).join("zero");
        }
        if let Some(path) = std::env::var_os("HOME") {
            return PathBuf::from(path)
                .join(".local")
                .join("state")
                .join("zero");
        }
        std::env::temp_dir().join("zero").join("state")
    }
    #[cfg(not(any(windows, unix)))]
    {
        std::env::temp_dir().join("zero").join("state")
    }
}

fn fnv1a(value: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in value {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

use std::io;
use std::net::IpAddr;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::RouteInterface;

#[derive(Debug, Deserialize)]
pub(super) struct RouteJournal {
    pub(super) tun_name: String,
    pub(super) ipv6: bool,
    pub(super) tun_index: u32,
    pub(super) egress: RouteInterface,
    #[serde(default)]
    pub(super) gateway: Option<String>,
    pub(super) excluded: Vec<IpAddr>,
    pub(super) installed: Vec<String>,
    #[serde(default)]
    pub(super) scoped_bypass: bool,
    #[serde(skip)]
    pub(super) path: PathBuf,
    #[serde(skip)]
    pub(super) _lease: Option<RouteLease>,
    #[cfg(target_os = "macos")]
    #[serde(skip)]
    pub(super) source: Option<crate::macos_privilege::MacosRouteFileAccess>,
    #[serde(skip)]
    pub(super) _metadata: metadata::Owner,
    #[serde(skip)]
    pub(super) _removed_routes: usize,
    #[serde(skip)]
    pub(super) _removed_exclusions: usize,
}

mod gateway;
pub(in crate::route) use gateway::Gateway;
mod cleanup;
mod decode;
mod lease;
pub(super) mod metadata;
mod object;
mod serialize;
pub(super) use lease::RouteLease;
mod material;
#[cfg(test)]
mod tests;

impl RouteJournal {
    pub(super) fn load(lease: &RouteLease, ipv6: bool) -> io::Result<Option<Self>> {
        #[cfg(target_os = "macos")]
        let _context = lease.context()?;
        let path = &lease.journal_path;
        let memory = lease.metadata();
        let raw = match material::read(path, memory.as_ref()) {
            Ok(raw) => raw,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let decoded = match memory {
            Some(memory) => match decode::decode(&raw, memory) {
                Ok(journal) => Ok(journal),
                Err(decode::Failure::Memory(source)) => return Err(io::Error::other(source)),
                Err(decode::Failure::Json(source)) => Err(source),
            },
            None => serde_json::from_slice(&raw),
        };
        let mut journal: Self = decoded.map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "parse TUN route recovery journal `{}`: {error}",
                    path.display()
                ),
            )
        })?;
        if journal.ipv6 != ipv6 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "TUN route recovery journal `{}` address-family mismatch",
                    path.display()
                ),
            ));
        }
        journal.path = journal._metadata.path(path).map_err(io::Error::other)?;
        journal._lease = None;
        #[cfg(target_os = "macos")]
        {
            journal.source = lease.source();
        }
        Ok(Some(journal))
    }

    #[cfg(any(target_os = "macos", test))]
    pub(super) fn record_scoped_bypass(&mut self) -> io::Result<()> {
        self.scoped_bypass = true;
        self.persist()
    }

    #[cfg(any(target_os = "macos", test))]
    pub(super) fn forget_scoped_bypass(&mut self) -> io::Result<()> {
        self.scoped_bypass = false;
        self.persist_or_clear()
    }

    fn persist_or_clear(&self) -> io::Result<()> {
        if !serialize::visible(&self.installed, self._removed_routes)
            && !serialize::visible(&self.excluded, self._removed_exclusions)
            && !self.scoped_bypass
        {
            self.clear()
        } else {
            self.persist()
        }
    }

    fn persist(&self) -> io::Result<()> {
        #[cfg(target_os = "macos")]
        let _context = self.cleanup_context()?;
        let encoded = material::encode(self, self._metadata.view())?;
        let temporary = self.path.with_extension("json.tmp");
        material::write(&temporary, &encoded)?;
        #[cfg(windows)]
        if self.path.exists() {
            std::fs::remove_file(&self.path)?;
        }
        std::fs::rename(&temporary, &self.path)
    }

    fn clear(&self) -> io::Result<()> {
        #[cfg(target_os = "macos")]
        let _context = self.cleanup_context()?;
        match std::fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }
}

fn route_journal_path(recovery_key: &str, ipv6: bool) -> io::Result<PathBuf> {
    let root = route_state_root()?;
    let safe_name = safe_recovery_key(recovery_key);
    Ok(root.join(format!(
        "routes-{safe_name}-{}.json",
        if ipv6 { "v6" } else { "v4" }
    )))
}

fn route_family_lock_path(ipv6: bool) -> io::Result<PathBuf> {
    Ok(route_state_root()?.join(format!(
        "routes-{}.owner.lock",
        if ipv6 { "v6" } else { "v4" }
    )))
}

fn safe_recovery_key(recovery_key: &str) -> String {
    let safe_name: String = recovery_key
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character
            } else {
                '_'
            }
        })
        .take(64)
        .collect();
    if safe_name.is_empty() {
        "tun".to_owned()
    } else {
        safe_name
    }
}

fn lock_owner_path(lock_path: &Path) -> PathBuf {
    lock_path.with_extension("")
}

fn persist_lock_owner(owner_path: &Path, owner: &str) -> io::Result<()> {
    material::write(owner_path, safe_recovery_key(owner).as_bytes())
}

pub(super) fn route_state_root() -> io::Result<PathBuf> {
    if let Some(path) = std::env::var_os("ZERO_TUN_STATE_DIR") {
        return create_private_state_dir(Path::new(&path));
    }
    #[cfg(windows)]
    let preferred = std::env::var_os("LOCALAPPDATA")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir)
        .join("Zero")
        .join("run");
    #[cfg(unix)]
    let preferred = std::env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/run"))
        .join("zero");
    match create_private_state_dir(&preferred) {
        Ok(path) => Ok(path),
        Err(_) => create_private_state_dir(&std::env::temp_dir().join("zero")),
    }
}

fn create_private_state_dir(path: &Path) -> io::Result<PathBuf> {
    std::fs::create_dir_all(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(path.to_path_buf())
}

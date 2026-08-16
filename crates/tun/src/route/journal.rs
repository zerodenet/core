use std::io;
use std::net::IpAddr;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::RouteInterface;

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct RouteJournal {
    pub(super) tun_name: String,
    pub(super) ipv6: bool,
    pub(super) tun_index: u32,
    pub(super) egress: RouteInterface,
    #[serde(default)]
    pub(super) gateway: Option<String>,
    pub(super) excluded: Vec<IpAddr>,
    pub(super) installed: Vec<String>,
    #[serde(skip)]
    pub(super) path: PathBuf,
    #[serde(skip)]
    pub(super) _lease: Option<RouteLease>,
}

#[derive(Debug)]
pub(super) struct RouteLease {
    journal_path: PathBuf,
    _lock: std::fs::File,
}

impl RouteLease {
    pub(super) fn acquire(recovery_key: &str, ipv6: bool) -> io::Result<Self> {
        let journal_path = route_journal_path(recovery_key, ipv6)?;
        Self::acquire_at(journal_path, recovery_key)
    }

    pub(super) fn acquire_at(journal_path: PathBuf, tun_name: &str) -> io::Result<Self> {
        let lock_path = journal_path.with_extension("lock");
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)?;
        fs2::FileExt::try_lock_exclusive(&lock).map_err(|error| {
            io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!(
                    "TUN route transaction for `{tun_name}` is already active (`{}`): {error}",
                    lock_path.display()
                ),
            )
        })?;
        Ok(Self {
            journal_path,
            _lock: lock,
        })
    }
}

impl RouteJournal {
    pub(super) fn load(lease: &RouteLease, ipv6: bool) -> io::Result<Option<Self>> {
        let path = lease.journal_path.clone();
        let raw = match std::fs::read(&path) {
            Ok(raw) => raw,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let mut journal: Self = serde_json::from_slice(&raw).map_err(|error| {
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
        journal.path = path;
        journal._lease = None;
        Ok(Some(journal))
    }

    pub(super) fn new(
        lease: RouteLease,
        tun_name: &str,
        ipv6: bool,
        tun_index: u32,
        egress: RouteInterface,
        gateway: Option<String>,
    ) -> io::Result<Self> {
        Ok(Self {
            tun_name: tun_name.to_owned(),
            ipv6,
            tun_index,
            egress,
            gateway,
            excluded: Vec::new(),
            installed: Vec::new(),
            path: lease.journal_path.clone(),
            _lease: Some(lease),
        })
    }

    pub(super) fn record_exclusion(&mut self, address: IpAddr) -> io::Result<()> {
        if !self.excluded.contains(&address) {
            self.excluded.push(address);
        }
        self.persist()
    }

    pub(super) fn forget_exclusion(&mut self, address: IpAddr) -> io::Result<()> {
        self.excluded.retain(|item| *item != address);
        self.persist_or_clear()
    }

    pub(super) fn replace_egress(
        &mut self,
        egress: RouteInterface,
        gateway: Option<String>,
    ) -> io::Result<()> {
        self.egress = egress;
        self.gateway = gateway;
        self.persist()
    }

    pub(super) fn record_route(&mut self, prefix: &str) -> io::Result<()> {
        self.installed.push(prefix.to_owned());
        self.persist()
    }

    pub(super) fn cleanup(
        &mut self,
        mut remove_route: impl FnMut(&str) -> io::Result<()>,
        mut remove_exclusion: impl FnMut(IpAddr) -> io::Result<()>,
    ) -> io::Result<()> {
        let mut first_error = None;
        for prefix in self.installed.clone().into_iter().rev() {
            match remove_route(&prefix) {
                Ok(()) => {
                    self.installed.retain(|item| item != &prefix);
                    if let Err(error) = self.persist_or_clear() {
                        first_error.get_or_insert(error);
                    }
                }
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }
        for address in self.excluded.clone().into_iter().rev() {
            match remove_exclusion(address) {
                Ok(()) => {
                    self.excluded.retain(|item| *item != address);
                    if let Err(error) = self.persist_or_clear() {
                        first_error.get_or_insert(error);
                    }
                }
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }
        if self.installed.is_empty() && self.excluded.is_empty() {
            if let Err(error) = self.clear() {
                first_error.get_or_insert(error);
            }
        } else if let Err(error) = self.persist() {
            first_error.get_or_insert(error);
        }
        first_error.map_or(Ok(()), Err)
    }

    fn persist_or_clear(&self) -> io::Result<()> {
        if self.installed.is_empty() && self.excluded.is_empty() {
            self.clear()
        } else {
            self.persist()
        }
    }

    fn persist(&self) -> io::Result<()> {
        let encoded = serde_json::to_vec(self).map_err(io::Error::other)?;
        let temporary = self.path.with_extension("json.tmp");
        std::fs::write(&temporary, encoded)?;
        #[cfg(windows)]
        if self.path.exists() {
            std::fs::remove_file(&self.path)?;
        }
        std::fs::rename(&temporary, &self.path)
    }

    fn clear(&self) -> io::Result<()> {
        match std::fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }
}

fn route_journal_path(recovery_key: &str, ipv6: bool) -> io::Result<PathBuf> {
    let root = route_state_root()?;
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
    Ok(root.join(format!(
        "routes-{}-{}.json",
        if safe_name.is_empty() {
            "tun"
        } else {
            &safe_name
        },
        if ipv6 { "v6" } else { "v4" }
    )))
}

fn route_state_root() -> io::Result<PathBuf> {
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

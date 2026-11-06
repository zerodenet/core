//! The existing persistent writer lease; inspection never acquires it.
use super::*;
use std::fs::{OpenOptions, TryLockError};
#[derive(Debug)]
pub(crate) struct PersistentStateLease {
    _file: crate::resources::FileView,
}

impl PersistentStateLease {
    pub(crate) fn acquire(
        path: &Path,
        resources: &crate::resources::DispatcherResources,
    ) -> ConnectorResult<Self> {
        let lock_path = lock_path(path);
        if let Some(parent) = lock_path.parent() {
            std::fs::create_dir_all(parent).map_err(|source| {
                ConnectorError::LockPersistentState {
                    path: path.display().to_string(),
                    lock_path: lock_path.display().to_string(),
                    source,
                }
            })?;
        }
        let file = resources
            .open(|| {
                OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .read(true)
                    .write(true)
                    .open(&lock_path)
            })
            .map_err(|source| ConnectorError::LockPersistentState {
                path: path.display().to_string(),
                lock_path: lock_path.display().to_string(),
                source,
            })?;
        match file.try_lock() {
            Ok(()) => Ok(Self { _file: file }),
            Err(TryLockError::WouldBlock) => Err(ConnectorError::PersistentStateInUse {
                path: path.display().to_string(),
                lock_path: lock_path.display().to_string(),
            }),
            Err(TryLockError::Error(source)) => Err(ConnectorError::LockPersistentState {
                path: path.display().to_string(),
                lock_path: lock_path.display().to_string(),
                source,
            }),
        }
    }
}

fn lock_path(path: &Path) -> PathBuf {
    let mut lock_path = path.as_os_str().to_os_string();
    lock_path.push(".zero.lock");
    PathBuf::from(lock_path)
}

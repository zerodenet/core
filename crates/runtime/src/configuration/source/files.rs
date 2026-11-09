use sha2::{Digest, Sha256};
use std::{
    io::{self, Read, Write},
    path::{Path, PathBuf},
};

pub(super) fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
pub(super) fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(suffix);
    PathBuf::from(name)
}
pub(super) fn read_optional(
    path: &Path,
    owner: &super::super::io::ConfigurationIo,
) -> io::Result<Option<Vec<u8>>> {
    match owner.read(path) {
        Ok(raw) => Ok(Some(raw)),
        Err(error)
            if error.kind() == io::ErrorKind::NotFound && owner.ensure_available().is_ok() =>
        {
            Ok(None)
        }
        Err(error) => Err(error),
    }
}
pub(super) fn parent(path: &Path) -> &Path {
    path.parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}
pub(super) fn sync_parent(
    path: &Path,
    owner: &super::super::io::ConfigurationIo,
) -> io::Result<()> {
    #[cfg(unix)]
    {
        owner.with_file(
            || std::fs::File::open(parent(path)),
            |directory| {
                if !directory.metadata()?.is_dir() {
                    return Err(io::Error::new(
                        io::ErrorKind::NotADirectory,
                        "source parent is no longer the original directory",
                    ));
                }
                directory.sync_all()
            },
        )?;
    }
    #[cfg(not(unix))]
    let _ = (path, owner);
    Ok(())
}
pub(super) fn atomic_write(
    path: &Path,
    bytes: &[u8],
    permissions: Option<std::fs::Permissions>,
    owner: &super::super::io::ConfigurationIo,
) -> io::Result<()> {
    atomic_write_with(path, permissions, owner, |file| file.write_all(bytes))
}
fn atomic_write_with(
    path: &Path,
    permissions: Option<std::fs::Permissions>,
    owner: &super::super::io::ConfigurationIo,
    write: impl FnOnce(&mut zero_platform_tokio::file::OriginalFile) -> io::Result<()>,
) -> io::Result<()> {
    let temporary_path = std::cell::RefCell::new(None);
    owner.with_file(
        || {
            let (file, path) = tempfile::NamedTempFile::new_in(parent(path))?.into_parts();
            *temporary_path.borrow_mut() = Some(path);
            Ok(file)
        },
        |file| {
            if let Some(permissions) = permissions {
                std::fs::set_permissions(temporary_path.borrow().as_ref().unwrap(), permissions)?;
            }
            write(file)?;
            file.sync_all()
        },
    )?;
    temporary_path
        .into_inner()
        .unwrap()
        .persist(path)
        .map_err(|error| error.error)?;
    #[cfg(test)]
    super::tests::after_rename(path);
    sync_parent(path, owner)
}
pub(super) fn remove_sidecars(
    path: &Path,
    owner: &super::super::io::ConfigurationIo,
) -> io::Result<()> {
    for suffix in [".zero-last-good", ".zero-recovery"] {
        match std::fs::remove_file(sidecar(path, suffix)) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    sync_parent(path, owner)
}

// The recovery digest never needs a second full file Vec beside previous.
pub(super) fn digest_optional(
    path: &Path,
    owner: &super::super::io::ConfigurationIo,
    max: usize,
) -> io::Result<Option<String>> {
    let result = owner.with_file(
        || std::fs::File::open(path),
        |file| {
            if file.metadata()?.len() > u64::try_from(max).map_err(io::Error::other)? {
                return Err(super::memory::too_large());
            }
            let mut hash = Sha256::new();
            let mut count = 0usize;
            let mut chunk = [0; 8192];
            loop {
                let n = file.read(&mut chunk)?;
                if n == 0 {
                    break;
                }
                count = count
                    .checked_add(n)
                    .filter(|n| *n <= max)
                    .ok_or_else(super::memory::too_large)?;
                hash.update(&chunk[..n]);
            }
            Ok(format!("{:x}", hash.finalize()))
        },
    );
    match result {
        Ok(digest) => Ok(Some(digest)),
        Err(error)
            if error.kind() == io::ErrorKind::NotFound && owner.ensure_available().is_ok() =>
        {
            Ok(None)
        }
        Err(error) => Err(error),
    }
}
pub(super) fn atomic_write_record(
    path: &Path,
    record: &super::RecoveryRecord,
    permissions: Option<std::fs::Permissions>,
    owner: &super::super::io::ConfigurationIo,
) -> io::Result<()> {
    atomic_write_with(path, permissions, owner, |file| {
        serde_json::to_writer(file, record).map_err(json_error)
    })
}
pub(super) fn recovery_record(
    path: &Path,
    owner: &super::super::io::ConfigurationIo,
    max: usize,
) -> io::Result<Option<super::RecoveryRecord>> {
    let result = owner.with_file(
        || std::fs::File::open(sidecar(path, ".zero-recovery")),
        |file| {
            let length = file.metadata()?.len();
            if length > u64::try_from(max).map_err(io::Error::other)? {
                return Err(super::memory::too_large());
            }
            serde_json::from_reader((&mut *file).take(length)).map_err(json_error)
        },
    );
    match result {
        Ok(record) => Ok(Some(record)),
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::NotADirectory
            ) && owner.ensure_available().is_ok() =>
        {
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

fn json_error(error: serde_json::Error) -> io::Error {
    io::Error::new(error.io_error_kind().unwrap_or(io::ErrorKind::Other), error)
}

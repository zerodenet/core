use sha2::{Digest, Sha256};
use std::{
    io::{self, Write},
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
/// A sidecar under a non-directory parent cannot exist. This applies only to
/// optional recovery metadata, never to reading/writing the actual source.
pub(super) fn read_recovery(
    path: &Path,
    owner: &super::super::io::ConfigurationIo,
) -> io::Result<Option<Vec<u8>>> {
    match read_optional(&sidecar(path, ".zero-recovery"), owner) {
        Err(error)
            if error.kind() == io::ErrorKind::NotADirectory && owner.ensure_available().is_ok() =>
        {
            Ok(None)
        }
        result => result,
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
            file.write_all(bytes)?;
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

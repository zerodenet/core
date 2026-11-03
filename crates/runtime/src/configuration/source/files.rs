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
pub(super) fn read_optional(path: &Path) -> io::Result<Option<Vec<u8>>> {
    match std::fs::read(path) {
        Ok(raw) => Ok(Some(raw)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}
/// A sidecar under a non-directory parent cannot exist. This applies only to
/// optional recovery metadata, never to reading/writing the actual source.
pub(super) fn read_recovery(path: &Path) -> io::Result<Option<Vec<u8>>> {
    match read_optional(&sidecar(path, ".zero-recovery")) {
        Err(error) if error.kind() == io::ErrorKind::NotADirectory => Ok(None),
        result => result,
    }
}

pub(super) fn parent(path: &Path) -> &Path {
    path.parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}
pub(super) fn sync_parent(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        let directory = std::fs::File::open(parent(path))?;
        if !directory.metadata()?.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::NotADirectory,
                "source parent is no longer the original directory",
            ));
        }
        directory.sync_all()?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}
pub(super) fn atomic_write(
    path: &Path,
    bytes: &[u8],
    permissions: Option<std::fs::Permissions>,
) -> io::Result<()> {
    let mut temporary = tempfile::NamedTempFile::new_in(parent(path))?;
    if let Some(permissions) = permissions {
        temporary.as_file().set_permissions(permissions)?;
    }
    temporary.write_all(bytes)?;
    temporary.as_file_mut().sync_all()?;
    temporary.persist(path).map_err(|error| error.error)?;
    #[cfg(test)]
    super::tests::after_rename(path);
    sync_parent(path)
}
pub(super) fn remove_sidecars(path: &Path) -> io::Result<()> {
    for suffix in [".zero-last-good", ".zero-recovery"] {
        match std::fs::remove_file(sidecar(path, suffix)) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    sync_parent(path)
}

//! Immutable pathname installation follows the original file's explicit close.
use super::*;
fn remove(path: &Path) -> io::Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}
pub(super) fn publish(
    attempt: &mut Attempt,
    io: &super::super::super::io::ConfigurationIo,
) -> io::Result<()> {
    assert!(attempt.native_closed);
    if attempt.path.try_exists()? {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(io::Error::other)?
            .as_nanos();
        let name = attempt
            .path
            .file_name()
            .ok_or_else(|| io::Error::other("cache destination has no file name"))?;
        let mut backup = std::ffi::OsString::from(".");
        backup.push(name);
        backup.push(format!(".previous.{}.{nonce}", std::process::id()));
        let backup = attempt.path.with_file_name(backup);
        std::fs::rename(&attempt.path, &backup)?;
        attempt.backup = Some(backup);
    }
    let temporary = attempt
        .temporary
        .take()
        .expect("actual downloaded original pathname");
    match temporary.persist(&attempt.path) {
        Ok(()) => attempt.installed = true,
        Err(error) => {
            attempt.temporary = Some(error.path);
            return Err(error.error);
        }
    }
    #[cfg(test)]
    super::tests::after_install(&attempt.path);
    super::super::files::sync_parent(&attempt.path, io)?;
    attempt.committed = true;
    cleanup_backup(attempt)?;
    super::super::files::sync_parent(&attempt.path, io)
}
fn cleanup_backup(attempt: &mut Attempt) -> io::Result<()> {
    if let Some(path) = &attempt.backup {
        remove(path)?;
    }
    attempt.backup = None;
    Ok(())
}
pub(super) fn abort(
    attempt: &mut Attempt,
    io: &super::super::super::io::ConfigurationIo,
) -> io::Result<()> {
    assert!(attempt.native_closed);
    if !attempt.committed {
        if attempt.installed {
            remove(&attempt.path)?;
            attempt.installed = false;
        }
        if let Some(backup) = &attempt.backup {
            std::fs::rename(backup, &attempt.path)?;
            attempt.backup = None;
        }
    } else {
        // The complete replacement and its first directory sync already
        // succeeded. A later backup-cleanup fault cannot undo that real fact.
        cleanup_backup(attempt)?;
    }
    if let Some(temporary) = &attempt.temporary {
        remove(temporary)?;
    }
    attempt.temporary = None;
    super::super::files::sync_parent(&attempt.path, io)
}

//! Preserve the exact prior pathname during an explicit journal rotation.
use super::*;
impl JournalGrant {
    /// Move only the declared journal to a sibling with a bounded single-name
    /// suffix. No path traversal, arbitrary rename target, handle or new root.
    /// The caller owns interpretation of its prior bytes, never this provider.
    pub fn preserve_previous(&self, suffix: &str) -> io::Result<Option<PathBuf>> {
        if suffix.is_empty()
            || suffix.len() > 128
            || !suffix
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "journal rotation suffix must be a bounded filename component",
            ));
        }
        self.0.require_scope()?;
        let mut state = self.0.call_state()?;
        self.0.require_open(&state)?;
        let read_closed = state.read.close();
        let append_closed = state.append.close();
        read_closed
            .and(append_closed)
            .map_err(|source| slots::failure(None, Some(source)))?;
        let mut name = self
            .0
            .spec
            .path
            .file_name()
            .expect("validated journal filename")
            .to_os_string();
        name.push(".");
        name.push(suffix);
        let destination = self.0.spec.path.with_file_name(name);
        // Do not replace a previous preserved original on a timestamp collision.
        match std::fs::symlink_metadata(&destination) {
            Ok(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "journal prior-state name already exists",
                ))
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        match std::fs::rename(&self.0.spec.path, &destination) {
            Ok(()) => Ok(Some(destination)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }
}

#[cfg(not(windows))]
pub(super) fn persist(
    path: tempfile::TempPath,
    destination: &std::path::Path,
) -> Result<(), (tempfile::TempPath, io::Error)> {
    path.persist(destination)
        .map_err(|error| (error.path, error.error))
}
#[cfg(windows)]
pub(super) fn persist(
    mut path: tempfile::TempPath,
    destination: &std::path::Path,
) -> Result<(), (tempfile::TempPath, io::Error)> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{
        MoveFileExW, MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH,
    };
    let from = path
        .as_os_str()
        .encode_wide()
        .chain(Some(0))
        .collect::<Vec<_>>();
    let to = destination
        .as_os_str()
        .encode_wide()
        .chain(Some(0))
        .collect::<Vec<_>>();
    // Same sole temporary file was sync_all'ed and positively closed before
    // entering this exact-path replace. Preserve the existing durable flag.
    if unsafe {
        MoveFileExW(
            from.as_ptr(),
            to.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    } == 0
    {
        return Err((path, io::Error::last_os_error()));
    }
    path.disable_cleanup(true);
    Ok(())
}

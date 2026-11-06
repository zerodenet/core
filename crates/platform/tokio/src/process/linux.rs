//! Exact retained /proc originals; table and directory data are streamed.
use super::{LocalProcessInfo, LocalProcessLookup, LocalProcessTransport, SocketAddr};
use crate::contract::{ChannelError, Impact, Reason, Stage};
use std::{path::PathBuf, sync::Arc};
mod directory;
mod slots;
mod table;
pub(in crate::process) use slots::Originals;
use slots::{DESCRIPTORS, FILE, ROOT};

pub(in crate::process) fn lookup(
    original: &LocalProcessLookup,
) -> Result<Option<LocalProcessInfo>, Arc<ChannelError>> {
    let mut originals = original.native.originals();
    if !originals.confirmed() {
        return Err(originals
            .failure()
            .unwrap_or_else(|| cause(Stage::Close, Reason::Io)));
    }
    let result = find(&mut originals, original);
    if let Err(source) = &result {
        originals.retain_operation_failure(source.clone());
    }
    // Native failures take priority; both original typed sources stay in this
    // finite record. No faulted slot is evicted/reopened.
    if let Err(source) = originals.close_all() {
        return Err(source);
    }
    match result {
        Err(source) if source.reason != Reason::Capacity && source.stage != Stage::Close => {
            Ok(None)
        }
        result => result,
    }
}
fn find(
    originals: &mut Originals,
    original: &LocalProcessLookup,
) -> Result<Option<LocalProcessInfo>, Arc<ChannelError>> {
    let limit = original.read_limit();
    let mut root = directory::Cursor::new(limit)?;
    let Some(inode) = table::inode(originals, original.source(), original.transport(), limit)?
    else {
        return Ok(None);
    };
    let target = format!("socket:[{inode}]");
    if originals.open(ROOT, &PathBuf::from("/proc"), true).is_err() {
        return Ok(None);
    }
    while let Some(name) = root.next(originals, ROOT)? {
        if original.cancelled() {
            return Ok(None);
        }
        let Ok(pid) = name.parse::<u32>() else {
            continue;
        };
        if pid == 0 {
            continue;
        }
        let path = PathBuf::from(format!("/proc/{pid}/fd"));
        if originals.open(DESCRIPTORS, &path, true).is_err() {
            continue;
        }
        let mut descriptors = directory::Cursor::new(limit)?;
        let mut found = false;
        while let Some(name) = descriptors.next(originals, DESCRIPTORS)? {
            if original.cancelled() {
                break;
            }
            if let Some(link) = read_link(&path.join(name), limit)? {
                if link == target {
                    found = true;
                    break;
                }
            }
        }
        originals.close(DESCRIPTORS)?;
        if found {
            let name = table::name(originals, pid, limit)?;
            let path = read_link(&PathBuf::from(format!("/proc/{pid}/exe")), limit)?;
            return Ok(Some(LocalProcessInfo { pid, name, path }));
        }
    }
    Ok(None)
}
fn read_link(path: &std::path::Path, limit: usize) -> Result<Option<String>, Arc<ChannelError>> {
    use std::os::unix::ffi::OsStrExt;
    let path = std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(|_| invalid())?;
    // readlinkat does not acquire a descriptor or borrow a directory cursor.
    let mut bytes = vec![0_u8; limit.min(libc::PATH_MAX as usize)];
    let size = unsafe {
        libc::readlinkat(
            libc::AT_FDCWD,
            path.as_ptr(),
            bytes.as_mut_ptr().cast(),
            bytes.len(),
        )
    };
    if size < 0 {
        return Ok(None);
    }
    let size = size as usize;
    if size == bytes.len() {
        return Err(capacity());
    }
    bytes.truncate(size);
    Ok(Some(String::from_utf8_lossy(&bytes).into_owned()))
}
fn capacity() -> Arc<ChannelError> {
    cause(Stage::Prepare, Reason::Capacity)
}
fn invalid() -> Arc<ChannelError> {
    cause(Stage::Prepare, Reason::Protocol)
}
fn io_failure(source: std::io::Error) -> Arc<ChannelError> {
    Arc::new(ChannelError::new(Stage::Prepare, Reason::Io, Impact::Session).with_source(source))
}
fn cause(stage: Stage, reason: Reason) -> Arc<ChannelError> {
    Arc::new(ChannelError::new(
        stage,
        reason,
        if stage == Stage::Close {
            Impact::Component
        } else {
            Impact::Session
        },
    ))
}

//! No pipe wait/open retry and no reads until the actual disk handle is checked.
use super::*;
use std::{
    fs::OpenOptions,
    os::windows::{fs::MetadataExt, io::AsRawHandle},
    path::{Component, Prefix},
};
use windows_sys::Win32::{
    Foundation::{GetLastError, SetLastError, ERROR_SUCCESS},
    Storage::FileSystem::{GetFileType, FILE_ATTRIBUTE_DEVICE, FILE_TYPE_DISK, FILE_TYPE_UNKNOWN},
};

pub(super) fn open(path: &Path) -> io::Result<File> {
    // Do not open explicit device/NT namespaces (including physical volumes and
    // local named pipes). A UNC pipe share is also not an ordinary file grant.
    // This validates syntax only; it does not canonicalize or grant a new path.
    let mut components = path.components();
    if let Some(Component::Prefix(prefix)) = components.next() {
        let unsupported = match prefix.kind() {
            Prefix::DeviceNS(_) | Prefix::Verbatim(_) => true,
            // `\\?\C:` addresses a volume, whereas `\\?\C:\file` is a
            // filesystem path. Do not pass the bare volume form to open.
            Prefix::VerbatimDisk(_) => !matches!(components.next(), Some(Component::RootDir)),
            Prefix::UNC(_, share) | Prefix::VerbatimUNC(_, share) => share
                .to_str()
                .is_some_and(|share| share.eq_ignore_ascii_case("pipe")),
            _ => false,
        };
        if unsupported {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "device and pipe namespaces are not regular-file grants",
            ));
        }
    }
    // std uses OPEN_EXISTING without WaitNamedPipe. In particular it never waits
    // or retries ERROR_PIPE_BUSY. A reparse target still needs the actual handle
    // check below before any metadata or read operation.
    OpenOptions::new().read(true).open(path)
}

pub(super) fn validate(file: &File) -> io::Result<()> {
    // SAFETY: File owns a live handle for this call. GetFileType does not take
    // ownership. Clear last-error so a successful UNKNOWN is distinguishable.
    let kind = unsafe {
        SetLastError(ERROR_SUCCESS);
        GetFileType(file.as_raw_handle())
    };
    if kind == FILE_TYPE_UNKNOWN {
        let code = unsafe { GetLastError() };
        if code != ERROR_SUCCESS {
            return Err(io::Error::from_raw_os_error(code as i32));
        }
    }
    if kind != FILE_TYPE_DISK {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "declared resource is not an opened disk file",
        ));
    }
    if file.metadata()?.file_attributes() & FILE_ATTRIBUTE_DEVICE != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "declared resource has device attributes",
        ));
    }
    // Handle metadata (never path metadata) excludes directories and rejects
    // disk-like handles without file metadata, before bounded ReadFile calls.
    Ok(())
}

#[cfg(test)]
pub(super) fn test_read_opened(file: File) -> io::Result<Vec<u8>> {
    let owner = PendingFileRead::new(Path::new("already-opened").to_path_buf(), 64)?;
    owner.adopt_for_test(file);
    let result = owner.run_adopted_for_test();
    drop(owner);
    result.map_err(|source| {
        Arc::try_unwrap(source)
            .unwrap_or_else(|source| io::Error::new(source.kind(), RetainedIo(source)))
    })
}

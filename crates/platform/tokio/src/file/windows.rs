//! No pipe wait/open retry and no reads until the actual disk handle is checked.
use super::*;
use std::{
    fs::OpenOptions,
    os::windows::{fs::MetadataExt, io::AsRawHandle},
    path::{Component, Prefix},
};
use windows_sys::Win32::{
    Foundation::{ERROR_SUCCESS, GetLastError, SetLastError},
    Storage::FileSystem::{FILE_ATTRIBUTE_DEVICE, FILE_TYPE_DISK, FILE_TYPE_UNKNOWN, GetFileType},
};

pub(super) fn read(path: &Path, max_bytes: usize, limit: usize) -> io::Result<Vec<u8>> {
    // Do not open explicit device/NT namespaces (including physical volumes and
    // local named pipes). A UNC pipe share is also not an ordinary file grant.
    // This validates syntax only; it does not canonicalize or grant a new path.
    if let Some(Component::Prefix(prefix)) = path.components().next() {
        let unsupported = match prefix.kind() {
            Prefix::DeviceNS(_) | Prefix::Verbatim(_) => true,
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
    let file = OpenOptions::new().read(true).open(path)?;
    read_opened_disk_file(file, max_bytes, limit)
}

fn read_opened_disk_file(file: File, max_bytes: usize, limit: usize) -> io::Result<Vec<u8>> {
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
    read_opened_file(file, max_bytes, limit)
}

#[cfg(test)]
pub(super) fn test_read_opened(file: File) -> io::Result<Vec<u8>> {
    read_opened_disk_file(file, 64, 65)
}

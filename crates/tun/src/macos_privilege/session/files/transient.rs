//! One reused read/write File original in the same Source's actual epoch.
use super::*;
use std::{ffi::CStr, os::unix::ffi::OsStrExt};
pub(super) struct NativePath {
    bytes: [u8; libc::PATH_MAX as usize],
    length: usize,
}
impl NativePath {
    pub(super) fn value(&self) -> &CStr {
        CStr::from_bytes_with_nul(&self.bytes[..self.length]).expect("single validated terminator")
    }
}
pub(super) fn native_path(path: &Path) -> io::Result<NativePath> {
    let bytes = path.as_os_str().as_bytes();
    if bytes.contains(&0) {
        return Err(io::Error::from(io::ErrorKind::InvalidInput));
    }
    let length = bytes
        .len()
        .checked_add(1)
        .ok_or_else(|| io::Error::from_raw_os_error(libc::ENAMETOOLONG))?;
    let mut value = NativePath {
        bytes: [0; libc::PATH_MAX as usize],
        length,
    };
    if length > value.bytes.len() {
        return Err(io::Error::from_raw_os_error(libc::ENAMETOOLONG));
    }
    value.bytes[..bytes.len()].copy_from_slice(bytes);
    Ok(value)
}
impl MacosHelperOwner {
    pub(in crate::macos_privilege::session) fn read_file(
        &self,
        path: &Path,
    ) -> io::Result<Vec<u8>> {
        self.file_io(path, None)
    }
    pub(in crate::macos_privilege::session) fn write_file(
        &self,
        path: &Path,
        bytes: &[u8],
    ) -> io::Result<()> {
        if bytes.len() > self.0.output_limit {
            return Err(io::Error::other(
                "route journal exceeds actual file material allowance",
            ));
        }
        self.file_io(path, Some(bytes)).map(drop)
    }
    fn file_io(&self, path: &Path, input: Option<&[u8]>) -> io::Result<Vec<u8>> {
        if !self.0.io_allowed() {
            return Err(io::Error::from(io::ErrorKind::Interrupted));
        }
        let _notifications = self.0.memory.defer_notifications();
        let path = native_path(path)?;
        let index = self.0.current.load(Ordering::Acquire);
        let mut epoch = self.0.epochs[index]
            .lock()
            .unwrap_or_else(|source| source.into_inner());
        if epoch
            .local_file
            .as_ref()
            .is_some_and(|original| !original.closed())
        {
            return Err(io::Error::other(
                "prior transient File original has no close receipt",
            ));
        }
        let flags = if input.is_some() {
            libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC
        } else {
            libc::O_RDONLY
        };
        let fd = unsafe { libc::open(path.value().as_ptr(), flags | libc::O_CLOEXEC, 0o600) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        epoch.local_file = Some(Original::new(fd, epoch.id));
        let result = match input {
            Some(bytes) => write_all(fd, bytes).map(|()| Vec::new()),
            None => read_all(fd, self.0.output_limit),
        };
        let closed = epoch.local_file.as_mut().unwrap().close_once();
        if let Err(source) = closed {
            self.0.failures.retain(index, Phase::RouteTransient, source);
            return Err(failures::retained(self.0.failures.clone().into_error()));
        }
        result
    }
}
fn write_all(fd: i32, mut bytes: &[u8]) -> io::Result<()> {
    while !bytes.is_empty() {
        let count = unsafe { libc::write(fd, bytes.as_ptr().cast(), bytes.len()) };
        if count < 0 {
            let source = io::Error::last_os_error();
            if source.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(source);
        }
        if count == 0 {
            return Err(io::Error::from(io::ErrorKind::WriteZero));
        }
        bytes = &bytes[count as usize..];
    }
    Ok(())
}
fn read_all(fd: i32, limit: usize) -> io::Result<Vec<u8>> {
    let mut output = Vec::new();
    loop {
        let mut bytes = [0u8; 4096];
        let count = unsafe { libc::read(fd, bytes.as_mut_ptr().cast(), bytes.len()) };
        if count < 0 {
            let source = io::Error::last_os_error();
            if source.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(source);
        }
        if count == 0 {
            return Ok(output);
        }
        if output
            .len()
            .checked_add(count as usize)
            .is_none_or(|len| len > limit)
        {
            return Err(io::Error::other(
                "route journal exceeds actual file material allowance",
            ));
        }
        output.extend_from_slice(&bytes[..count as usize]);
    }
}

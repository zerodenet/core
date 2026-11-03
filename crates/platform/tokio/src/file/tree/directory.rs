//! The controller is installed before acquisition. fdopendir transfers the same
//! original descriptor; failure retains it for explicit cleanup, never Drop proof.
use super::*;
#[cfg(unix)]
pub(super) struct Directory {
    file: Option<File>,
    pending: Option<std::os::fd::RawFd>,
    dir: *mut libc::DIR,
}
// Every operation is exclusive under FileTreeNative's mutex. The native cursor
// is moved between blocking workers, never accessed by concurrent threads.
#[cfg(unix)]
unsafe impl Send for Directory {}
#[cfg(unix)]
impl Directory {
    pub(super) fn new() -> Self {
        Self {
            file: None,
            pending: None,
            dir: std::ptr::null_mut(),
        }
    }
    pub(super) fn acquire(&mut self, path: &Path) -> io::Result<()> {
        use std::os::fd::IntoRawFd;
        self.file = Some(super::super::native::open(path)?);
        let fd = self.file.take().unwrap().into_raw_fd();
        self.pending = Some(fd);
        let dir = unsafe { libc::fdopendir(fd) };
        if dir.is_null() {
            return Err(io::Error::last_os_error());
        }
        self.dir = dir;
        self.pending = None;
        Ok(())
    }
    pub(super) fn entries(&mut self, path: &Path) -> io::Result<Vec<String>> {
        use std::os::unix::ffi::OsStrExt;
        let mut names = Vec::new();
        loop {
            #[cfg(any(target_os = "macos", target_os = "ios", target_os = "freebsd"))]
            unsafe {
                *libc::__error() = 0;
            }
            #[cfg(target_os = "linux")]
            unsafe {
                *libc::__errno_location() = 0;
            }
            #[cfg(target_os = "android")]
            unsafe {
                *libc::__errno() = 0;
            }
            let entry = unsafe { libc::readdir(self.dir) };
            if entry.is_null() {
                let source = io::Error::last_os_error();
                if source.raw_os_error().unwrap_or(0) != 0 {
                    return Err(source);
                }
                break;
            }
            let entry = unsafe { &*entry };
            let name = unsafe { std::ffi::CStr::from_ptr(entry.d_name.as_ptr()) };
            if name.to_bytes() == b"." || name.to_bytes() == b".." {
                continue;
            }
            let directory =
                std::fs::symlink_metadata(path.join(std::ffi::OsStr::from_bytes(name.to_bytes())))?
                    .is_dir();
            let mut name = name.to_string_lossy().into_owned();
            if directory {
                name.push('/');
            }
            names.push(name);
            if names.len() > 65536 {
                return Err(io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "directory entry bound exceeded",
                ));
            }
        }
        Ok(names)
    }
    pub(super) fn close(mut self) -> io::Result<bool> {
        if !self.dir.is_null() {
            let dir = std::mem::replace(&mut self.dir, std::ptr::null_mut());
            return if unsafe { libc::closedir(dir) } == 0 {
                Ok(true)
            } else {
                Err(io::Error::last_os_error())
            };
        }
        if let Some(fd) = self.pending.take() {
            return if unsafe { libc::close(fd) } == 0 {
                Ok(true)
            } else {
                Err(io::Error::last_os_error())
            };
        }
        if let Some(file) = self.file.take() {
            super::super::native::close(file)?;
            return Ok(true);
        }
        Ok(false)
    }
}
#[cfg(unix)]
impl Drop for Directory {
    fn drop(&mut self) {
        if !self.dir.is_null() {
            unsafe {
                libc::closedir(self.dir);
            }
        } else if let Some(fd) = self.pending.take() {
            unsafe {
                libc::close(fd);
            }
        }
    }
}
#[cfg(windows)]
mod windows;
#[cfg(windows)]
pub(super) use windows::Directory;
#[cfg(not(any(unix, windows)))]
pub(super) struct Directory;
#[cfg(not(any(unix, windows)))]
impl Directory {
    pub(super) fn new() -> Self {
        Self
    }
    pub(super) fn acquire(&mut self, _: &Path) -> io::Result<()> {
        Err(io::ErrorKind::Unsupported.into())
    }
    pub(super) fn entries(&mut self, _: &Path) -> io::Result<Vec<String>> {
        Err(io::ErrorKind::Unsupported.into())
    }
    pub(super) fn close(self) -> io::Result<bool> {
        Ok(false)
    }
}

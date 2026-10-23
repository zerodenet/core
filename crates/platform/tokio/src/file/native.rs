use super::*;

pub(super) fn open(path: &Path) -> io::Result<File> {
    #[cfg(unix)]
    {
        use std::{fs::OpenOptions, os::unix::fs::OpenOptionsExt};
        OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(path)
    }
    #[cfg(windows)]
    {
        windows::open(path)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = path;
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "no regular-file open policy",
        ))
    }
}

// Consume the sole original descriptor exactly once. A failing close never
// reconstructs a File or retries a possibly reused descriptor.
pub(super) fn close(file: File) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::fd::IntoRawFd;
        let fd = file.into_raw_fd();
        if unsafe { libc::close(fd) } == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }
    #[cfg(windows)]
    {
        use std::os::windows::io::IntoRawHandle;
        let handle = file.into_raw_handle();
        if unsafe { windows_sys::Win32::Foundation::CloseHandle(handle) } != 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }
    #[cfg(not(any(unix, windows)))]
    {
        std::mem::forget(file);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "no explicit file close policy",
        ))
    }
}

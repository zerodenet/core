//! Adopt an existing dedicated host L3 device without configuring the host.
use std::io;
#[cfg(unix)]
fn duplicate(fd: i32) -> io::Result<std::os::fd::OwnedFd> {
    use std::os::fd::FromRawFd;
    if fd < 3 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "dedicated packet descriptor must be >= 3",
        ));
    }
    // SAFETY: fcntl duplicates a supplied descriptor; the new descriptor is uniquely owned.
    let new = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 3) };
    if new < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { std::os::fd::OwnedFd::from_raw_fd(new) })
}
/// The caller guarantees exclusive packet I/O on the supplied TUN handle.
/// A duplicate is owned by Zero; the original descriptor remains host-owned.
pub fn adopt(fd: i32, expected_interface: &str) -> io::Result<impl crate::TunDevice> {
    #[cfg(target_os = "linux")]
    {
        crate::LinuxTun::adopt(duplicate(fd)?, expected_interface)
    }
    #[cfg(target_os = "macos")]
    {
        crate::Utun::adopt(duplicate(fd)?, expected_interface)
    }
}

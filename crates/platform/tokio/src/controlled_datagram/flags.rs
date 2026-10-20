//! Preserve Socket::new flags without its post-create implicit-Drop window.
#[cfg(not(windows))]
pub(super) fn creation_type() -> socket2::Type {
    let ty = socket2::Type::DGRAM;
    #[cfg(any(
        target_os = "android",
        target_os = "dragonfly",
        target_os = "freebsd",
        target_os = "fuchsia",
        target_os = "hurd",
        target_os = "illumos",
        target_os = "linux",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "cygwin",
    ))]
    let ty = ty.cloexec();
    ty
}
pub(super) fn configure(socket: &socket2::SockRef<'_>) -> std::io::Result<()> {
    #[cfg(unix)]
    socket.set_cloexec(true)?;
    #[cfg(target_vendor = "apple")]
    socket.set_nosigpipe(true)?;
    // Windows NO_HANDLE_INHERIT was set atomically in WSASocketW above.
    #[cfg(not(unix))]
    let _ = socket;
    Ok(())
}

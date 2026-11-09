//! Fixed authorization script and flags, no new execution or lifecycle owner.
use super::*;
pub(super) fn set_flags(fd: RawFd) -> io::Result<()> {
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) } < 0 {
        return Err(io::Error::last_os_error());
    }
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
        return Err(io::Error::last_os_error());
    }
    let enabled = 1_i32;
    if unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_NOSIGPIPE,
            (&enabled as *const i32).cast(),
            std::mem::size_of_val(&enabled) as _,
        )
    } < 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}
pub(in crate::macos_privilege::session) const OWNED_SCRIPT: &str = r#"
on run argv
    if (count of argv) is 0 then error "missing privileged command"
    set commandText to quoted form of (item 1 of argv)
    set nonceText to item (count of argv) of argv
    repeat with argumentIndex from 2 to ((count of argv) - 1)
        set commandText to commandText & " " & quoted form of (item argumentIndex of argv)
    end repeat
    try
        do shell script commandText with administrator privileges
        return "zero.utun.completed." & nonceText
    on error errorText number errorNumber
        if errorNumber is -128 then return "zero.utun.cancelled." & nonceText
        error errorText number errorNumber
    end try
end run
"#;

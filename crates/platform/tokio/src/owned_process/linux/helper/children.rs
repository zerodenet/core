//! Signal only actual unreaped children of this sole-waiter supervisor.
//! Subreaping adopts each next generation after its parents have been reaped.
pub(super) unsafe fn signal_owned() -> i32 {
    unsafe {
        let fd = libc::open(
            c"/proc/thread-self/children".as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC,
        );
        // /proc/thread-self/children is not present on all kernels; use the
        // task directory, prepared with decimal syscall PID on the stack.
        let fd = if fd >= 0 { fd } else { open_children() };
        if fd < 0 {
            return *libc::__errno_location();
        }
        let mut bytes = [0u8; 4096];
        let mut child = 0i32;
        loop {
            let read = libc::read(fd, bytes.as_mut_ptr().cast(), bytes.len());
            if read < 0 {
                if *libc::__errno_location() == libc::EINTR {
                    continue;
                }
                let error = *libc::__errno_location();
                libc::close(fd);
                return error;
            }
            let mut index = 0;
            while index < read as usize {
                let byte = bytes[index];
                if byte.is_ascii_digit() {
                    match child
                        .checked_mul(10)
                        .and_then(|n| n.checked_add((byte - b'0') as i32))
                    {
                        Some(pid) => child = pid,
                        None => {
                            libc::close(fd);
                            return libc::EOVERFLOW;
                        }
                    }
                } else if child > 0 {
                    signal(child);
                    child = 0;
                }
                index += 1;
            }
            if read == 0 {
                if child > 0 {
                    signal(child);
                }
                break;
            }
        }
        libc::close(fd);
        0
    }
}
unsafe fn signal(child: i32) {
    unsafe {
        let mut info: libc::siginfo_t = std::mem::zeroed();
        // A live or zombie child is still ours until this sole helper reaps
        // it. No handler/other waiter can release and recycle its PID here.
        if libc::waitid(
            libc::P_PID,
            child as u32,
            &mut info,
            libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
        ) == 0
        {
            libc::kill(child, libc::SIGKILL);
        }
    }
}
unsafe fn open_children() -> i32 {
    unsafe {
        let mut path = [0u8; 64];
        let prefix = b"/proc/self/task/";
        path[..prefix.len()].copy_from_slice(prefix);
        let mut digits = [0u8; 16];
        let mut pid = libc::getpid() as u32;
        let mut count = 0;
        while pid != 0 {
            digits[count] = b'0' + (pid % 10) as u8;
            count += 1;
            pid /= 10;
        }
        let mut position = prefix.len();
        while count > 0 {
            count -= 1;
            path[position] = digits[count];
            position += 1;
        }
        let suffix = b"/children\0";
        path[position..position + suffix.len()].copy_from_slice(suffix);
        libc::open(path.as_ptr().cast(), libc::O_RDONLY | libc::O_CLOEXEC)
    }
}

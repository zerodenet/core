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
            return scan_proc();
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
/// CONFIG_CHECKPOINT_RESTORE is optional: some kernels expose no children
/// file. Enumerate only numeric PIDs, then ask the kernel whether each is our
/// actual unreaped child. This sole waiter cannot release/recycle its identity
/// between that native custody check and the signal. Nonchildren are untouched.
unsafe fn scan_proc() -> i32 {
    unsafe {
        let directory = libc::open(
            c"/proc".as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
        );
        if directory < 0 {
            return *libc::__errno_location();
        }
        let mut bytes = [0u8; 4096];
        loop {
            let size = libc::syscall(
                libc::SYS_getdents64,
                directory,
                bytes.as_mut_ptr(),
                bytes.len(),
            );
            if size < 0 {
                if *libc::__errno_location() == libc::EINTR {
                    continue;
                }
                let error = *libc::__errno_location();
                libc::close(directory);
                return error;
            }
            if size == 0 {
                break;
            }
            let mut offset = 0;
            while offset < size as usize {
                if offset + 19 > size as usize {
                    libc::close(directory);
                    return libc::EIO;
                }
                let length = u16::from_ne_bytes([bytes[offset + 16], bytes[offset + 17]]) as usize;
                if length < 20 || offset + length > size as usize {
                    libc::close(directory);
                    return libc::EIO;
                }
                let mut position = offset + 19;
                let mut pid = 0i32;
                let mut valid = false;
                while position < offset + length {
                    let byte = bytes[position];
                    if byte == 0 {
                        break;
                    }
                    if !byte.is_ascii_digit() {
                        valid = false;
                        break;
                    }
                    match pid
                        .checked_mul(10)
                        .and_then(|n| n.checked_add((byte - b'0') as i32))
                    {
                        Some(value) => pid = value,
                        None => {
                            valid = false;
                            break;
                        }
                    }
                    valid = true;
                    position += 1;
                }
                if valid && pid > 0 {
                    signal(pid);
                }
                offset += length;
            }
        }
        libc::close(directory);
        0
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

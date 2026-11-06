//! Explicitly remove all copied core descriptors before the root is forked.
pub(super) unsafe fn prepare(control: i32, null: i32) -> bool {
    unsafe {
        let ctl = libc::fcntl(control, libc::F_DUPFD_CLOEXEC, 10);
        if ctl < 0 {
            return false;
        }
        let devnull = libc::fcntl(null, libc::F_DUPFD_CLOEXEC, 10);
        if devnull < 0 {
            return false;
        }
        if libc::dup3(ctl, 3, libc::O_CLOEXEC) < 0 || libc::dup3(devnull, 4, libc::O_CLOEXEC) < 0 {
            return false;
        }
        let mut fd = 0;
        while fd < 3 {
            if libc::dup2(4, fd) < 0 {
                return false;
            }
            fd += 1;
        }
        close_from(5)
    }
}

pub(super) unsafe fn close_from(first: i32) -> bool {
    unsafe {
        if libc::syscall(libc::SYS_close_range, first as u32, u32::MAX, 0) == 0 {
            return true;
        }
        // Older kernels: enumerate actual fd entries instead of relying on
        // RLIMIT_NOFILE (which may have been lowered below an inherited fd).
        let directory = libc::open(
            c"/proc/self/fd".as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
        );
        if directory < 0 {
            return false;
        }
        let mut buffer = [0u8; 4096];
        loop {
            let size = libc::syscall(
                libc::SYS_getdents64,
                directory,
                buffer.as_mut_ptr(),
                buffer.len(),
            );
            if size < 0 {
                if *libc::__errno_location() == libc::EINTR {
                    continue;
                }
                libc::close(directory);
                return false;
            }
            if size == 0 {
                break;
            }
            let mut offset = 0;
            while offset < size as usize {
                if offset + 19 > size as usize {
                    libc::close(directory);
                    return false;
                }
                let record =
                    u16::from_ne_bytes([buffer[offset + 16], buffer[offset + 17]]) as usize;
                if record < 20 || offset + record > size as usize {
                    libc::close(directory);
                    return false;
                }
                let mut position = offset + 19;
                let mut number = 0i32;
                let mut valid = false;
                while position < offset + record {
                    let byte = buffer[position];
                    if byte == 0 {
                        break;
                    }
                    if !byte.is_ascii_digit() {
                        valid = false;
                        break;
                    }
                    match number
                        .checked_mul(10)
                        .and_then(|value| value.checked_add((byte - b'0') as i32))
                    {
                        Some(value) => number = value,
                        None => {
                            valid = false;
                            break;
                        }
                    }
                    valid = true;
                    position += 1;
                }
                if valid && number >= first && number != directory {
                    libc::close(number);
                }
                offset += record;
            }
        }
        libc::close(directory);
        true
    }
}

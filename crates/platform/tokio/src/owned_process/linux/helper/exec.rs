use super::{RawLaunch, isolation};

pub(super) unsafe fn root(gate: i32, errors: i32, launch: RawLaunch) -> ! {
    unsafe {
        let mut byte = 0u8;
        let read = loop {
            let read = libc::read(gate, (&mut byte as *mut u8).cast(), 1);
            if read < 0 && *libc::__errno_location() == libc::EINTR {
                continue;
            }
            break read;
        };
        if read != 1 || byte != 1 {
            libc::_exit(125);
        }
        libc::close(gate);
        // Duplicate before replacing fd3, which is the private supervisor
        // channel. No root executable can inherit any supervisor endpoint.
        let report = libc::fcntl(errors, libc::F_DUPFD_CLOEXEC, 10);
        if report < 0 || libc::dup3(report, 3, libc::O_CLOEXEC) < 0 {
            libc::_exit(125);
        }
        if !isolation::close_from(4) {
            fail(*libc::__errno_location());
        }
        let mut signal = 1;
        while signal < 65 {
            if signal != libc::SIGKILL && signal != libc::SIGSTOP {
                let original = &*launch.signals.add(signal as usize);
                let mut action: libc::sigaction = std::mem::zeroed();
                libc::sigemptyset(&mut action.sa_mask);
                action.sa_sigaction =
                    if original.sa_sigaction == libc::SIG_IGN && signal != libc::SIGPIPE {
                        libc::SIG_IGN
                    } else {
                        libc::SIG_DFL
                    };
                libc::sigaction(signal, &action, std::ptr::null_mut());
            }
            signal += 1;
        }
        libc::sigprocmask(libc::SIG_SETMASK, &launch.mask, std::ptr::null_mut());
        let mut index = 0;
        let mut denied = false;
        while index < launch.count {
            libc::execve(*launch.paths.add(index), launch.argv, launch.envp);
            let error = *libc::__errno_location();
            if error == libc::ENOEXEC {
                libc::execve(
                    c"/bin/sh".as_ptr(),
                    (*launch.shells.add(index)).as_ptr(),
                    launch.envp,
                );
                fail(*libc::__errno_location());
            }
            if error == libc::EACCES {
                denied = true;
            } else if error != libc::ENOENT && error != libc::ENOTDIR {
                fail(error);
            }
            index += 1;
        }
        fail(if denied { libc::EACCES } else { libc::ENOENT });
    }
}
unsafe fn fail(error: i32) -> ! {
    unsafe {
        libc::write(3, (&error as *const i32).cast(), 4);
        libc::_exit(127);
    }
}

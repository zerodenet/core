//! The fork child is a syscall-only supervisor, never a Tokio executor.
//! All storage is bounded stack storage or immutable parent-prepared memory.
use super::{prepare::RawLaunch, wire};
mod children;
mod exec;
mod isolation;

unsafe fn errno() -> i32 {
    unsafe { *libc::__errno_location() }
}
unsafe fn fault(fd: i32, token: u64, error: i32) {
    unsafe {
        wire::send(fd, wire::Frame::new(token, wire::FAULT, error, 0), None);
    }
}
unsafe fn command(fd: i32, token: u64, expected: i32) -> bool {
    unsafe {
        match wire::receive(fd) {
            Ok(message) => {
                let mut index = 0;
                while index < message.count {
                    libc::close(message.descriptors[index]);
                    index += 1;
                }
                !message.eof
                    && !message.malformed
                    && message.count == 0
                    && message.frame.matches(token)
                    && message.frame.kind() == expected
            }
            Err(_) => false,
        }
    }
}

/// Never returns through Rust stack destructors in the post-fork child.
pub(super) unsafe fn run(control: i32, null: i32, token: u64, launch: RawLaunch) -> ! {
    unsafe {
        if !isolation::prepare(control, null) {
            libc::_exit(125);
        }
        let fd = 3;
        if libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) < 0 {
            fault(fd, token, errno());
            libc::_exit(125);
        }
        // This process is the sole waiter for its own children. Default
        // SIGCHLD (without NOCLDWAIT) preserves every unreaped child identity.
        let mut action: libc::sigaction = std::mem::zeroed();
        libc::sigemptyset(&mut action.sa_mask);
        let mut signal = 1;
        while signal < 65 {
            if signal != libc::SIGKILL && signal != libc::SIGSTOP {
                action.sa_sigaction = if signal == libc::SIGCHLD {
                    libc::SIG_DFL
                } else {
                    libc::SIG_IGN
                };
                libc::sigaction(signal, &action, std::ptr::null_mut());
            }
            signal += 1;
        }
        let mut mask = std::mem::zeroed();
        libc::sigemptyset(&mut mask);
        libc::sigprocmask(libc::SIG_SETMASK, &mask, std::ptr::null_mut());
        if !command(fd, token, wire::BEGIN) {
            wire::send(fd, wire::Frame::new(token, wire::TREE_ENDED, 0, 0), None);
            libc::_exit(0); // no root has been created
        }
        let mut gate = [-1; 2];
        let mut error_pipe = [-1; 2];
        if libc::pipe2(gate.as_mut_ptr(), libc::O_CLOEXEC) < 0 {
            fault(fd, token, errno());
            wire::send(fd, wire::Frame::new(token, wire::TREE_ENDED, 0, 0), None);
            libc::_exit(125);
        }
        if libc::pipe2(error_pipe.as_mut_ptr(), libc::O_CLOEXEC) < 0 {
            fault(fd, token, errno());
            wire::send(fd, wire::Frame::new(token, wire::TREE_ENDED, 0, 0), None);
            libc::_exit(125);
        }
        let root = libc::fork();
        if root == 0 {
            libc::close(gate[1]);
            libc::close(error_pipe[0]);
            exec::root(gate[0], error_pipe[1], launch);
        }
        libc::close(gate[0]);
        libc::close(error_pipe[1]);
        if root < 0 {
            fault(fd, token, errno());
            wire::send(fd, wire::Frame::new(token, wire::TREE_ENDED, 0, 0), None);
            libc::_exit(125);
        }
        let pidfd = libc::syscall(libc::SYS_pidfd_open, root, 0) as i32;
        if pidfd < 0 && errno() != libc::ENOSYS && errno() != libc::EINVAL {
            fault(fd, token, errno());
        }
        let sent = wire::send(
            fd,
            wire::Frame::new(token, wire::ROOT_HANDLE, 0, root),
            (pidfd >= 0).then_some(pidfd),
        ) == 0;
        if pidfd >= 0 {
            libc::close(pidfd);
        }
        let admitted = sent && command(fd, token, wire::ROOT_INSTALLED);
        if admitted {
            let byte = 1u8;
            libc::write(gate[1], (&byte as *const u8).cast(), 1);
        }
        libc::close(gate[1]);
        let mut execution_error = 0i32;
        let size = loop {
            let size = libc::read(error_pipe[0], (&mut execution_error as *mut i32).cast(), 4);
            if size < 0 && errno() == libc::EINTR {
                continue;
            }
            break size;
        };
        libc::close(error_pipe[0]);
        let mut stopping = !admitted || size != 0;
        if size > 0 {
            fault(fd, token, execution_error);
        }
        if !stopping {
            wire::send(fd, wire::Frame::new(token, wire::STARTED, 0, root), None);
        }
        let mut root_status = None;
        loop {
            if stopping {
                let error = children::signal_owned();
                if error != 0 {
                    fault(fd, token, error);
                }
            }
            let mut status = 0;
            let waited = libc::waitpid(-1, &mut status, libc::WNOHANG);
            if waited > 0 {
                if waited == root {
                    root_status = Some(status);
                    stopping = true;
                    wire::send(
                        fd,
                        wire::Frame::new(token, wire::ROOT_EXIT, status, root),
                        None,
                    );
                }
                continue;
            }
            if waited < 0 && errno() == libc::ECHILD {
                if let Some(status) = root_status {
                    wire::send(
                        fd,
                        wire::Frame::new(token, wire::TREE_ENDED, status, root),
                        None,
                    );
                    libc::_exit(0);
                }
                fault(fd, token, libc::ECHILD);
                libc::_exit(125);
            }
            if waited < 0 && errno() != libc::EINTR {
                fault(fd, token, errno());
            }
            let mut poll = libc::pollfd {
                fd,
                events: libc::POLLIN,
                revents: 0,
            };
            let polled = libc::poll(&mut poll, 1, 100);
            if polled > 0 {
                // EOF/malformed/foreign frames stop business but never count
                // as closure. Only the actual ECHILD path emits TREE_ENDED.
                if poll.revents & (libc::POLLIN | libc::POLLHUP | libc::POLLERR) != 0 {
                    stopping = true;
                    let _ = command(fd, token, wire::STOP);
                }
            }
        }
    }
}

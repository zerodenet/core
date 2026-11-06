use super::*;
impl OwnedProcess {
    /// Called by the actual registered actor, after its witness has been
    /// installed by the host. The root cannot execute before that handoff.
    pub fn start(&self, actor: TaskJoinWitness) -> io::Result<()> {
        if !self.scope.resource_open()
            || !self.scope.same_scope(&actor.scope_identity())
            || actor.is_joined()
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "process actor is not in its open original Scope",
            ));
        }
        if let Some(endpoint) = self.endpoint() {
            endpoint.check_start(&actor, self)?;
        }
        let mut state = self.lock();
        if state.start_attempted
            || state.stop
            || (state.endpoint.is_some() && !state.endpoint_request_ready)
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "process start is terminal",
            ));
        }
        if let Some(actual) = &state.actor {
            if !actual.same_registration(&actor) {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "process start crossed its bound original actor",
                ));
            }
        } else {
            state.actor = Some(actor);
        }
        state.start_attempted = true;
        let result = self.start_inner(&mut state);
        if let Err(error) = result {
            let retained = io::Error::new(error.kind(), error);
            state.fault(retained);
            state.stop = true;
            state.request = None;
            return Err(io::Error::other(SharedFailure(
                state.first_error.clone().expect("retained original error"),
            )));
        }
        Ok(())
    }
    fn start_inner(&self, state: &mut State) -> io::Result<()> {
        let launch = prepare::Launch::prepare(
            state
                .request
                .as_ref()
                .ok_or_else(|| io::Error::other("process request already consumed"))?,
        )?;
        let mut sockets = [-1; 2];
        if unsafe {
            libc::socketpair(
                libc::AF_UNIX,
                libc::SOCK_SEQPACKET | libc::SOCK_CLOEXEC,
                0,
                sockets.as_mut_ptr(),
            )
        } < 0
        {
            return Err(io::Error::last_os_error());
        }
        let parent = state.retain(sockets[0]);
        let child = state.retain(sockets[1]);
        state.control_index = Some(parent);
        let null = unsafe { libc::open(c"/dev/null".as_ptr(), libc::O_RDWR | libc::O_CLOEXEC) };
        if null < 0 {
            return Err(io::Error::last_os_error());
        }
        let null_index = state.retain(null);
        let mut blocked = unsafe { std::mem::zeroed() };
        let mut previous = unsafe { std::mem::zeroed() };
        unsafe {
            libc::sigfillset(&mut blocked);
        }
        let code = unsafe { libc::pthread_sigmask(libc::SIG_SETMASK, &blocked, &mut previous) };
        if code != 0 {
            return Err(io::Error::from_raw_os_error(code));
        }
        let pid = unsafe { libc::fork() };
        if pid == 0 {
            unsafe {
                helper::run(sockets[1], null, self.token, launch.raw());
            }
        }
        let fork_error = if pid < 0 {
            Some(io::Error::last_os_error())
        } else {
            None
        };
        // Install the real helper identity before any following fallible IO.
        if pid > 0 {
            state.helper = Some(pid);
        }
        let restored =
            unsafe { libc::pthread_sigmask(libc::SIG_SETMASK, &previous, std::ptr::null_mut()) };
        state.close(child);
        state.close(null_index);
        if let Some(error) = fork_error {
            return Err(error);
        }
        if restored != 0 {
            return Err(io::Error::from_raw_os_error(restored));
        }
        let handle = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) } as i32;
        if handle >= 0 {
            state.helper_handle = Some(state.retain(handle));
        } else {
            let error = io::Error::last_os_error();
            if !matches!(
                error.raw_os_error(),
                Some(libc::ENOSYS) | Some(libc::EINVAL)
            ) {
                state.fault(error);
            }
        }
        if unsafe { libc::fcntl(sockets[0], libc::F_SETFL, libc::O_NONBLOCK) } < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: this same original remains in state.originals until finish
        // removes the registration, then explicitly closes the original fd.
        state.control = Some(
            unsafe { AsyncFd::register(View(sockets[0])) }
                .map_err(|failure| failure.into_parts().1)?,
        );
        let code = unsafe {
            wire::send(
                sockets[0],
                wire::Frame::new(self.token, wire::BEGIN, 0, 0),
                None,
            )
        };
        if code != 0 {
            return Err(wire::io_error(code));
        }
        state.request = None; // launch credentials no longer retained in parent
        Ok(())
    }
}
#[derive(Debug)]
pub(super) struct SharedFailure(pub(super) Arc<io::Error>);
impl std::fmt::Display for SharedFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for SharedFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

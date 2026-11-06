use super::*;
use std::task::{Context, Poll};

impl OwnedProcess {
    /// Monitor the original root for its actual lifetime. The startup deadline
    /// is not a lifetime limit. A root exit is never a tree/fd/Scope receipt.
    pub async fn wait_root_exit(&self) -> io::Result<ExitStatus> {
        use std::os::unix::process::ExitStatusExt;
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let progress = std::future::poll_fn(|cx| {
                let mut state = self.lock();
                self.progress(&mut state, cx);
                if let Some(status) = state.root_status {
                    return Poll::Ready(Ok(ExitStatus::from_raw(status)));
                }
                if state.helper_status.is_some() {
                    let source = state.first_error.clone().unwrap_or_else(|| {
                        Arc::new(io::Error::other(
                            "original supervisor ended without a root wait status",
                        ))
                    });
                    return Poll::Ready(Err(io::Error::other(start::SharedFailure(source))));
                }
                Poll::Pending
            });
            tokio::select! {
                result = progress => return result,
                _ = &mut changed => {},
                _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {},
            }
        }
    }
    /// Drives the same original. Dropping this waiter retains every acquired
    /// fd, message, native wait status and first failure in the original.
    pub async fn wait_started(&self, deadline: tokio::time::Instant) -> io::Result<()> {
        self.wait_for(deadline, false).await
    }
    /// Physical tree closure only. The actor can call this before returning;
    /// finish still requires its actual Scope join and explicit fd closes.
    pub async fn wait_tree_ended(&self, deadline: tokio::time::Instant) -> io::Result<()> {
        self.wait_for(deadline, true).await
    }
    async fn wait_for(&self, deadline: tokio::time::Instant, tree: bool) -> io::Result<()> {
        tokio::time::timeout_at(deadline, async {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                let ready = std::future::poll_fn(|cx| {
                    let mut state = self.lock();
                    self.progress(&mut state, cx);
                    if tree && physical(&state) {
                        return Poll::Ready(Ok(true));
                    }
                    if !tree {
                        if let Some(error) = state.first_error.clone() {
                            return Poll::Ready(Err(io::Error::other(start::SharedFailure(error))));
                        }
                        if state.started {
                            return Poll::Ready(Ok(true));
                        }
                        if state.stop || physical(&state) {
                            return Poll::Ready(Err(io::Error::new(
                                io::ErrorKind::Interrupted,
                                "process stopped before exec",
                            )));
                        }
                    }
                    Poll::Pending
                });
                tokio::select! {
                    result = ready => return result.map(|_| ()),
                    _ = &mut changed => {},
                    // Older kernels have no helper pidfd. The same actor
                    // polls its actual unreaped child; no provider task exists.
                    _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {},
                }
            }
        })
        .await
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                "original process wait deadline elapsed",
            )
        })?
    }
    fn progress(&self, state: &mut State, cx: &mut Context<'_>) {
        let Some(control) = state.control_index.and_then(|i| state.originals[i].raw) else {
            return;
        };
        if state.stop && !state.stop_sent {
            let error = unsafe {
                wire::send(
                    control,
                    wire::Frame::new(self.token, wire::STOP, 0, 0),
                    None,
                )
            };
            if error == 0 {
                state.stop_sent = true;
            } else if error != libc::EAGAIN {
                state.fault(wire::io_error(error));
            }
        }
        let mut remaining = 64;
        while !state.eof && remaining > 0 {
            remaining -= 1;
            match unsafe { wire::receive(control) } {
                Ok(message) => {
                    if message.eof {
                        state.eof = true;
                        break;
                    }
                    self.receive(state, message, control);
                }
                Err(libc::EAGAIN) => break,
                Err(error) => {
                    state.fault(wire::io_error(error));
                    state.stop = true;
                    break;
                }
            }
        }
        if !state.eof {
            if let Some(view) = &state.control {
                match view.poll_read_ready(cx) {
                    Poll::Ready(Ok(mut ready)) => {
                        ready.clear_ready();
                        // Rearm against the real current readiness after the
                        // bounded synchronous drain. No busy-loop on EOF.
                        if remaining == 0 {
                            cx.waker().wake_by_ref();
                        } else {
                            let _ = view.poll_read_ready(cx);
                        }
                    }
                    Poll::Ready(Err(error)) => {
                        state.fault(error);
                        state.stop = true;
                    }
                    Poll::Pending => {}
                }
            }
        }
        if let Some(helper) = state.helper.filter(|_| state.helper_status.is_none()) {
            let mut status = 0;
            let waited = unsafe { libc::waitpid(helper, &mut status, libc::WNOHANG) };
            if waited == helper {
                state.helper_status = Some(status);
            } else if waited < 0 {
                let error = io::Error::last_os_error();
                if error.raw_os_error() != Some(libc::EINTR) {
                    state.fault(error);
                }
            }
        }
        if state.helper_status.is_some() && state.eof && !state.tree {
            state.fault(io::Error::other(
                "process supervisor exited without tree attestation",
            ));
        }
    }
    fn receive(&self, state: &mut State, message: wire::Message, control: i32) {
        let mut descriptors = Vec::with_capacity(message.count);
        for index in 0..message.count {
            descriptors.push(state.retain(message.descriptors[index]));
        }
        if message.malformed || !message.frame.matches(self.token) {
            state.conversation_valid = false;
            state.stop = true;
            state.fault(io::Error::other("malformed original process conversation"));
            return;
        }
        let frame = message.frame;
        match frame.kind() {
            wire::ROOT_HANDLE
                if state.root.is_none() && frame.pid() > 0 && descriptors.len() <= 1 =>
            {
                state.root = Some(frame.pid());
                state.root_handle = descriptors.first().copied();
                let kind = if state.stop {
                    wire::STOP
                } else {
                    wire::ROOT_INSTALLED
                };
                let error =
                    unsafe { wire::send(control, wire::Frame::new(self.token, kind, 0, 0), None) };
                if error != 0 {
                    state.fault(wire::io_error(error));
                    state.stop = true;
                }
            }
            wire::STARTED if descriptors.is_empty() && state.root == Some(frame.pid()) => {
                state.started = true
            }
            wire::ROOT_EXIT
                if descriptors.is_empty()
                    && state.root == Some(frame.pid())
                    && state.root_status.is_none() =>
            {
                state.root_status = Some(frame.value());
            }
            wire::FAULT if descriptors.is_empty() && frame.value() > 0 => {
                state.fault(wire::io_error(frame.value()))
            }
            wire::TREE_ENDED
                if descriptors.is_empty()
                    && !state.tree
                    && ((state.root == Some(frame.pid())
                        && state.root_status == Some(frame.value()))
                        || (state.root.is_none() && frame.pid() == 0 && frame.value() == 0)) =>
            {
                state.tree = true
            }
            _ => {
                state.conversation_valid = false;
                state.stop = true;
                state.fault(io::Error::other("invalid original process transition"));
            }
        }
    }
}
pub(super) fn physical(state: &State) -> bool {
    if state.helper.is_none() {
        return state.actor.is_some();
    }
    state.tree
        && state.conversation_valid
        && state
            .helper_status
            .is_some_and(|status| libc::WIFEXITED(status))
}

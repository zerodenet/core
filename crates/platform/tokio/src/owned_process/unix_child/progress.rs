use super::*;
use tokio::time::Instant;
impl State {
    pub(super) fn progress(&mut self) {
        if self.root_status.is_some() {
            return;
        }
        let Some(child) = self.child.as_mut() else {
            return;
        };
        match child.try_wait() {
            Ok(Some(status)) => {
                self.root_status = Some(status);
                return;
            }
            Ok(None) => {}
            Err(source) => {
                self.fault(source);
                return;
            }
        }
        if self.stop && !self.stop_sent {
            match self.child.as_mut().unwrap().start_kill() {
                Ok(()) => self.stop_sent = true,
                Err(source) => self.fault(source),
            }
        }
    }
}
impl OwnedProcess {
    pub async fn wait_started(&self, deadline: Instant) -> io::Result<()> {
        if Instant::now() >= deadline {
            return Err(io::ErrorKind::TimedOut.into());
        }
        let mut state = self.lock();
        state.progress();
        let result = if let Some(source) = &state.first_error {
            Err(original_failure(source))
        } else if state.acquired && !state.stop && state.root_status.is_none() {
            Ok(())
        } else {
            Err(denied("native child is not running"))
        };
        drop(state);
        self.changed.notify_waiters();
        result
    }
    /// Poll the actual retained Child wait in the caller's existing actor.
    /// The temporary wait future borrows that same Child; cancellation cannot
    /// replace it or discard the native wait status retained in this owner.
    pub async fn wait_root_exit(&self) -> io::Result<ExitStatus> {
        use std::{future::Future, task::Poll};
        let result = loop {
            // Child::wait keeps one native waker. Both the original monitor
            // and outer cleanup may await it; broadcast the retained outcome
            // so consuming that status cannot strand the other original waiter.
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let native = std::future::poll_fn(|cx| {
                let mut state = self.lock();
                state.progress();
                if let Some(status) = state.root_status {
                    return Poll::Ready(Ok(status));
                }
                if let Some(source) = &state.first_error {
                    return Poll::Ready(Err(original_failure(source)));
                }
                let result = {
                    let Some(child) = state.child.as_mut() else {
                        return Poll::Ready(Err(denied("original child was never started")));
                    };
                    let waiting = child.wait();
                    tokio::pin!(waiting);
                    waiting.poll(cx)
                };
                match result {
                    Poll::Ready(Ok(status)) => {
                        state.root_status = Some(status);
                        Poll::Ready(Ok(status))
                    }
                    Poll::Ready(Err(source)) => {
                        state.fault(source);
                        Poll::Ready(Err(original_failure(state.first_error.as_ref().unwrap())))
                    }
                    Poll::Pending => Poll::Pending,
                }
            });
            tokio::select! {
                result = native => break result,
                _ = changed => {}
            }
        };
        self.changed.notify_waiters();
        if let Some(endpoint) = self.endpoint() {
            endpoint.request_stop();
        }
        result
    }
    pub async fn wait_child_ended(&self, deadline: Instant) -> io::Result<()> {
        let res = {
            let mut state = self.lock();
            state.progress();
            state.physical()
        };
        if res {
            self.changed.notify_waiters();
            return Ok(());
        }
        tokio::time::timeout_at(deadline, self.wait_root_exit())
            .await
            .map_err(|source| io::Error::new(io::ErrorKind::TimedOut, source))??;
        Ok(())
    }
}

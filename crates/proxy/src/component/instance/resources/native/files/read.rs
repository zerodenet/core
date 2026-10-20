use super::*;
use std::task::{Context, Poll, Wake, Waker};
struct CompletionWake(Arc<tokio::sync::Notify>);
impl Wake for CompletionWake {
    fn wake(self: Arc<Self>) {
        self.0.notify_waiters();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.notify_waiters();
    }
}
impl HostResources {
    pub(in super::super) async fn read_declared_file(
        &self,
        path: PathBuf,
        max_bytes: usize,
    ) -> Result<Vec<u8>, ChannelError> {
        let deadline = {
            let mut state = self.state.lock().expect("native grants poisoned");
            self.listener_phase(&state)?;
            state.files.register(self.component, &path, max_bytes)?;
            state.start_deadline.expect("checked start phase")
        };
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let state = self.state.lock().expect("native grants poisoned");
                self.listener_phase(&state)?;
                if let Cached::Done(result) = &state
                    .files
                    .grants
                    .get(&path)
                    .expect("registered file")
                    .cached
                {
                    let bytes = result.as_ref().map_err(|source| {
                        file_failure(self.component, Stage::Prepare, source.clone())
                    })?;
                    if bytes.len() > max_bytes {
                        return Err(error(
                            self.component,
                            Reason::Capacity,
                            "file contents exceed the requested bound",
                        ));
                    }
                    return Ok(bytes.to_vec());
                }
            }
            tokio::select! {biased;
                _=self.cancel.cancelled()=>return Err(error(self.component,Reason::Revoked,"native file owner was revoked")),
                _=tokio::time::sleep_until(deadline)=>return Err(error(self.component,Reason::Timeout,"native file read deadline elapsed")),
                _=self.next_file_exit()=>{},
                _=&mut changed=>{},
            }
        }
    }
    pub(in super::super) async fn next_file_exit(&self) -> bool {
        // JoinSet stores one waker. It must wake the actual owner fan-out, not
        // the most recent caller: that caller can disappear while others wait.
        let waker = Waker::from(Arc::new(CompletionWake(self.changed.clone())));
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let next = {
                let mut state = self.state.lock().expect("native grants poisoned");
                match state
                    .files
                    .jobs
                    .poll_join_next_with_id(&mut Context::from_waker(&waker))
                {
                    Poll::Ready(Some(result)) => {
                        state.files.record(self.component, result);
                        Poll::Ready(true)
                    }
                    Poll::Ready(None) => Poll::Ready(false),
                    Poll::Pending => Poll::Pending,
                }
            };
            if let Poll::Ready(result) = next {
                self.changed.notify_waiters();
                return result;
            }
            changed.await;
        }
    }
}

use super::*;
use std::task::{Context, Poll};
struct Polling<'a>(&'a Original);
impl Drop for Polling<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.poisoned.store(true, Ordering::Release);
        }
        self.0.polling.store(false, Ordering::Release);
    }
}
struct Continuation<'a> {
    entry: &'a Original,
    original: Option<BoxFuture<'static, Result<(), ChannelError>>>,
}
impl Drop for Continuation<'_> {
    fn drop(&mut self) {
        if let Some(original) = self.original.take() {
            *lock(&self.entry.cleanup) = Some(original);
        }
    }
}
impl EmbeddedCommandOwner {
    pub(in crate::embedded_commands) fn poll_frontends(&self, cx: &mut Context<'_>) -> Poll<()> {
        let entries = lock(&self.state).frontends.clone();
        let mut progress = false;
        for entry in &entries {
            if entry.poisoned.load(Ordering::Acquire) {
                continue;
            }
            if entry.polling.swap(true, Ordering::AcqRel) {
                continue;
            }
            let _poll = Polling(entry);
            if entry.raw_no_work.load(Ordering::Acquire) && !entry.creating.load(Ordering::Acquire)
            {
                // The original gated raw job itself observed its start channel
                // close before invoking work. This is not inferred from Drop.
                entry.cleanup_complete.store(true, Ordering::Release);
            }
            if !entry.creating.load(Ordering::Acquire)
                && entry.raw.get().is_none()
                && entry.caller_returned.load(Ordering::Acquire)
                && !entry.cleanup_complete.load(Ordering::Acquire)
                && lock(&entry.cleanup).is_none()
            {
                let observation = entry.observation.clone();
                *lock(&entry.cleanup) = Some(Box::pin(async move {
                    let joined = observation.wait_joined().await;
                    match observation.completion_fault(joined.as_ref()) {
                        Some(faults) => Err(fault(Reason::Internal).with_source(faults)),
                        None => Ok(()),
                    }
                }));
            }
            let original = lock(&entry.cleanup).take();
            if let Some(original) = original {
                let mut custody = Continuation {
                    entry,
                    original: Some(original),
                };
                match custody.original.as_mut().unwrap().as_mut().poll(cx) {
                    Poll::Ready(result) => {
                        drop(custody.original.take());
                        if let Err(source) = result {
                            entry.failure.get_or_init(|| Arc::new(source));
                        }
                        if !entry.poisoned.load(Ordering::Acquire) {
                            entry.cleanup_complete.store(true, Ordering::Release);
                        }
                        progress = true;
                    }
                    Poll::Pending => {}
                }
            }
            let mut ack = lock(&entry.caller_ack).take();
            if let Some(actual) = ack.as_mut() {
                if actual.as_mut().poll(cx).is_ready() {
                    entry.caller_returned.store(true, Ordering::Release);
                    ack = None;
                    progress = true;
                }
            }
            *lock(&entry.caller_ack) = ack;
            // Real Scope registration return is observed passively. Inline
            // commands have no separate child task and use their exact return.
            if let Some(Registration::Scoped(witness)) = entry.registration.get() {
                if witness
                    .try_joined()
                    .is_some_and(|j| j.registration_released())
                {
                    entry.caller_returned.store(true, Ordering::Release);
                }
            }
        }
        let mut returned = Vec::new();
        let mut returned_bytes = Vec::new();
        let mut removed = Vec::new();
        {
            let mut state = lock(&self.state);
            let mut index = 0;
            while index < state.frontends.len() {
                let entry = &state.frontends[index];
                if let Some(Registration::Scoped(witness)) = entry.registration.get() {
                    if witness
                        .try_joined()
                        .is_some_and(|j| j.registration_released())
                    {
                        entry.caller_returned.store(true, Ordering::Release);
                    }
                }

                let raw = entry
                    .raw
                    .get()
                    .map_or(!entry.creating.load(Ordering::Acquire), |w| {
                        w.try_joined().is_some_and(|j| j.registration_released())
                    });
                if raw
                    && entry.cleanup_complete.load(Ordering::Acquire)
                    && entry.caller_returned.load(Ordering::Acquire)
                {
                    returned.extend(lock(&entry.credit).take());
                    returned_bytes.extend(std::mem::take(&mut *lock(&entry.request_bytes)));
                    if entry.failure.get().is_none() && entry.raw_failure.get().is_none() {
                        removed.push(state.frontends.remove(index));
                        continue;
                    }
                }
                index += 1;
            }
        }
        drop(removed);
        drop(returned_bytes);
        drop(returned);
        if progress {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

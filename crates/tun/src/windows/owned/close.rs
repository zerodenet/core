//! Await the exact saved native reader, then consume the same session and adapter.
use super::*;
use std::sync::atomic::Ordering;
impl OwnedWintunOwner {
    pub fn stop(&self) {
        if !self.closing.swap(true, Ordering::AcqRel) {
            let mut state = self.state();
            if let Some(reader) = &state.prepared_reader {
                reader.grant.revoke();
            }
            self.reader_ready.notify_all();
            if let Some(session) = &state.session {
                if let Err(source) = session.shutdown() {
                    state.errors.push(Arc::new(io::Error::from(source)));
                }
            }
        }
    }
    /// Cancellation leaves the saved JoinHandle and all native originals in
    /// place. No blocking thread join occurs until that exact thread finished.
    pub async fn close(&self) -> Result<Option<Arc<TunCloseReceipt>>, Arc<io::Error>> {
        self.stop();
        loop {
            {
                let mut state = self.state();
                if let Some(source) = &state.native_failure {
                    return Err(source.clone());
                }
                if state.view_live {
                } else if state
                    .prepared_reader
                    .as_ref()
                    .is_some_and(|reader| !reader.done)
                {
                } else if state
                    .reader
                    .as_ref()
                    .is_some_and(|reader| !reader.is_finished())
                {
                } else {
                    if let Some(reader) = state.reader.take() {
                        let joined = reader
                            .join()
                            .map_err(|payload| {
                                Arc::new(io::Error::other(ThreadPanic(Mutex::new(Some(payload)))))
                            })
                            .and_then(|result| result.map_err(Arc::new));
                        if let Err(source) = &joined {
                            state.errors.push(source.clone());
                        }
                        state.reader_joined = Some(joined);
                    }
                    if let Some(session) = state.session.take() {
                        match Arc::try_unwrap(session) {
                            Err(original) => {
                                state.session = Some(original);
                            }
                            Ok(original) => match original.close_owned() {
                                Ok(receipt) => state.ended = Some(Arc::new(receipt)),
                                Err(source) => {
                                    state.native_failure = Some(Arc::new(io::Error::other(source)))
                                }
                            },
                        }
                    }
                    if state.session.is_none() {
                        if let Some(adapter) = state.adapter.take() {
                            match Arc::try_unwrap(adapter) {
                                Err(original) => state.adapter = Some(original),
                                Ok(original) => state.adapter_closed = Some(original.close_owned()),
                            }
                        }
                        if state.adapter.is_none() {
                            if let Some(source) = &state.native_failure {
                                return Err(source.clone());
                            }
                            if state.receipt.is_none() {
                                if let (Some(identity), Some(ended), Some(adapter)) = (
                                    state.identity.clone(),
                                    state.ended.clone(),
                                    state.adapter_closed.clone(),
                                ) {
                                    match TunCloseReceipt::from_wintun(
                                        state.name.clone(),
                                        identity,
                                        ended,
                                        adapter,
                                    ) {
                                        Ok(receipt) => state.receipt = Some(Arc::new(receipt)),
                                        Err(source) => {
                                            let source = Arc::new(source);
                                            state.native_failure = Some(source.clone());
                                            return Err(source);
                                        }
                                    }
                                }
                            }
                            if !state.errors.is_empty() {
                                return Err(Arc::new(io::Error::other(CloseErrors(
                                    state.errors.clone(),
                                ))));
                            }
                            return Ok(state.receipt.clone());
                        }
                    }
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    }
}
struct ThreadPanic(Mutex<Option<Box<dyn std::any::Any + Send>>>);
impl std::fmt::Debug for ThreadPanic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OriginalWintunReaderPanic")
            .field("payload_retained", &self.0.lock().unwrap().is_some())
            .finish()
    }
}
impl std::fmt::Display for ThreadPanic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}
impl std::error::Error for ThreadPanic {}
#[derive(Debug)]
struct CloseErrors(Vec<Arc<io::Error>>);
impl std::fmt::Display for CloseErrors {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (index, source) in self.0.iter().enumerate() {
            if index > 0 {
                f.write_str("; ")?;
            }
            source.fmt(f)?;
        }
        Ok(())
    }
}
impl std::error::Error for CloseErrors {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.first().map(|source| source.as_ref() as _)
    }
}

//! Synchronous chunk admission and explicit completion of the retained attempt.
use super::*;
impl RuleSetCacheDownload {
    fn record_unknown<T>(&self, result: io::Result<T>) -> io::Result<T> {
        if result.is_err() && self.source.io.has_retained_failure() {
            super::super::super::startup::retain_unknown(self.source.clone());
        }
        result
    }
    pub fn write_chunk(&self, bytes: &[u8]) -> io::Result<()> {
        self.record_unknown(self.write_chunk_owned(bytes))
    }
    fn write_chunk_owned(&self, bytes: &[u8]) -> io::Result<()> {
        let mut slot = lock(&self.source.cache);
        let attempt = self.attempt(&mut slot)?;
        if attempt.frozen {
            return Err(io::Error::other(
                "cache chunk admission stopped before final cleanup",
            ));
        }
        let next = attempt
            .bytes
            .checked_add(bytes.len() as u64)
            .filter(|size| *size <= MAX_RULE_SET_CACHE_BYTES);
        let Some(next) = next else {
            attempt.frozen = true;
            return Err(remember(
                attempt,
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("downloaded rule set exceeds {MAX_RULE_SET_CACHE_BYTES} bytes"),
                ),
            ));
        };
        if let Err(error) = self.source.io.write_cache_chunk(bytes) {
            attempt.frozen = true;
            return Err(remember(attempt, error));
        }
        attempt.bytes = next;
        Ok(())
    }
    pub fn finish(&self) -> io::Result<u64> {
        self.record_unknown(self.finish_owned())
    }
    fn finish_owned(&self) -> io::Result<u64> {
        let mut slot = lock(&self.source.cache);
        if slot.is_none() && self.identity.terminal.load(Ordering::Acquire) == COMMITTED {
            return Ok(self.identity.bytes.load(Ordering::Acquire));
        }
        let attempt = self.attempt(&mut slot)?;
        if let Some(error) = attempt.identity.failure.get() {
            return Err(retained(error));
        }
        attempt.frozen = true;
        let result = (|| {
            if !attempt.native_closed {
                let synchronized = self.source.io.close_cache_file(true)?;
                attempt.native_closed = true; // only the actual matching receipt issues outer Ok
                synchronized?;
            }
            if !attempt.namespace_complete {
                files::publish(attempt, &self.source.io)?;
                attempt.namespace_complete = true;
            }
            Ok::<(), io::Error>(())
        })();
        if let Err(error) = result {
            return Err(remember(attempt, error));
        }
        let bytes = attempt.bytes;
        super::super::super::startup::release_confirmed(&self.source)?;
        self.identity.bytes.store(bytes, Ordering::Release);
        self.identity.terminal.store(COMMITTED, Ordering::Release);
        *slot = None;
        Ok(bytes)
    }
    pub fn abort(&self) -> io::Result<()> {
        self.record_unknown(self.abort_owned())
    }
    fn abort_owned(&self) -> io::Result<()> {
        let mut slot = lock(&self.source.cache);
        if slot.is_none() && self.identity.terminal.load(Ordering::Acquire) != ACTIVE {
            return Ok(());
        }
        let attempt = self.attempt(&mut slot)?;
        attempt.frozen = true;
        let result = (|| {
            if !attempt.native_closed {
                self.source.io.close_cache_file(false)??;
                attempt.native_closed = true;
            }
            if !attempt.namespace_complete {
                files::abort(attempt, &self.source.io)?;
                attempt.namespace_complete = true;
            }
            Ok::<(), io::Error>(())
        })();
        if let Err(error) = result {
            return Err(remember(attempt, error));
        }
        let terminal = if attempt.committed {
            COMMITTED
        } else {
            ABORTED
        };
        let bytes = attempt.bytes;
        super::super::super::startup::release_confirmed(&self.source)?;
        self.identity.bytes.store(bytes, Ordering::Release);
        self.identity.terminal.store(terminal, Ordering::Release);
        *slot = None;
        Ok(())
    }
    fn attempt<'a>(&self, slot: &'a mut Option<Attempt>) -> io::Result<&'a mut Attempt> {
        slot.as_mut()
            .filter(|a| Arc::ptr_eq(&a.identity, &self.identity))
            .ok_or_else(|| {
                io::Error::other("cache view no longer has its original retained attempt")
            })
    }
}

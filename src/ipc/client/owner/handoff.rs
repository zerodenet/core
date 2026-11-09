//! Local copies retire only after the real receiver owns every exact source.
use super::*;
impl Attempt {
    pub(super) fn handoff(&self, driver: io::Result<()>) -> io::Result<()> {
        debug_assert!(self.retired.load(Ordering::Acquire));
        let driver = driver.err();
        let cleanup = self.faults.report().err();
        let abandoned = lock(&self.faults.abandoned).take();
        // Build the full strong receiver before removing any producer copy.
        // No await follows this transfer; cancellation cannot lose its owner.
        let received = Handoff {
            driver,
            cleanup,
            abandoned,
        };
        let driver_released = match received.driver.as_ref() {
            None => true,
            Some(error) => self.driver.release_joined_failure(error),
        };
        let joined = std::mem::take(&mut *lock(&self.faults.joined));
        let native = lock(&self.faults.native).take();
        drop((joined, native));
        let already = self.fault_delivered.swap(true, Ordering::AcqRel);
        if !already
            && (received.driver.is_some()
                || received.cleanup.is_some()
                || received.abandoned.is_some())
        {
            return Err(io::Error::other(received));
        }
        // Drop observation-local historical copies before a caller can prepare
        // smaller limits on this same book. External receivers retain theirs.
        drop(received);
        if !driver_released {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "same client joined-failure handoff is contended",
            ));
        }
        Ok(())
    }
}
pub(super) struct Handoff {
    driver: Option<io::Error>,
    cleanup: Option<io::Error>,
    abandoned: Option<Arc<io::Error>>,
}
impl std::fmt::Debug for Handoff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientHandoff")
            .field("driver", &self.driver.is_some())
            .field("cleanup", &self.cleanup.is_some())
            .field("abandoned", &self.abandoned.is_some())
            .finish()
    }
}
impl std::fmt::Display for Handoff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(error) = &self.driver {
            return std::fmt::Display::fmt(error, f);
        }
        if let Some(error) = &self.cleanup {
            return std::fmt::Display::fmt(error, f);
        }
        if let Some(error) = &self.abandoned {
            return std::fmt::Display::fmt(error, f);
        }
        f.write_str("control client cleanup failed")
    }
}
impl std::error::Error for Handoff {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.driver
            .as_ref()
            .map(|error| error as _)
            .or_else(|| self.cleanup.as_ref().map(|error| error as _))
            .or_else(|| self.abandoned.as_ref().map(|error| error.as_ref() as _))
    }
}

#[cfg(test)]
impl Handoff {
    pub(super) fn retained_tasks(&self) -> Option<&ClientFaults> {
        self.cleanup
            .as_ref()
            .and_then(|error| error.get_ref()?.downcast_ref::<ClientFaults>())
    }
}
#[cfg(all(test, unix))]
#[path = "handoff/tests.rs"]
mod tests;

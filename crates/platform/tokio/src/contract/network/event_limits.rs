//! Finite bounds on the existing concrete event-network owner; no IO authority.
use std::{io, num::NonZeroUsize};
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EventNetworkLimits {
    tasks: NonZeroUsize,
    originals: NonZeroUsize,
    lookups: NonZeroUsize,
}
impl EventNetworkLimits {
    pub fn new(max_tasks: usize, max_originals: usize, max_lookups: usize) -> io::Result<Self> {
        let limit = |value| {
            NonZeroUsize::new(value)
                .filter(|value| value.get() <= 4096)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "event network limit outside 1..4096",
                    )
                })
        };
        Ok(Self {
            tasks: limit(max_tasks)?,
            originals: limit(max_originals)?,
            lookups: limit(max_lookups)?,
        })
    }
    pub fn max_tasks(self) -> NonZeroUsize {
        self.tasks
    }
    pub fn max_originals(self) -> NonZeroUsize {
        self.originals
    }
    pub fn max_lookups(self) -> NonZeroUsize {
        self.lookups
    }
}
impl Default for EventNetworkLimits {
    fn default() -> Self {
        Self::new(1024, 256, 256).expect("finite nonzero event-network policy")
    }
}

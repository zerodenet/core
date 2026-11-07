//! One actual stream has exactly one terminal shutdown and one terminal read.
//! No retry-loop error list or native/task/Scope proof is stored here.
use super::*;
use std::{error::Error, fmt, sync::OnceLock};
use zero_platform_tokio::contract::CleanupFailureReceipt;
#[derive(Default)]
struct Sources {
    shutdown: OnceLock<Arc<ChannelError>>,
    read: OnceLock<Arc<ChannelError>>,
}
pub(super) struct Failures {
    sources: Arc<Sources>,
    receipt: CleanupFailureReceipt,
    canonical: OnceLock<Arc<ChannelError>>,
}
impl Default for Failures {
    fn default() -> Self {
        let sources = Arc::new(Sources::default());
        let receipt = CleanupFailureReceipt::new();
        assert!(receipt.bind_payload(sources.clone()));
        Self {
            sources,
            receipt,
            canonical: Default::default(),
        }
    }
}
impl Failures {
    pub(super) fn shutdown(&self, source: ChannelError) {
        assert!(
            self.sources.shutdown.set(Arc::new(source)).is_ok(),
            "one actual shutdown error; this direction must not be repolled"
        );
    }
    pub(super) fn read(&self, source: ChannelError) {
        assert!(
            self.sources.read.set(Arc::new(source)).is_ok(),
            "one actual read error; this direction must not be repolled"
        );
    }
    pub(super) fn has_failure(&self) -> bool {
        self.sources.shutdown.get().is_some() || self.sources.read.get().is_some()
    }
    pub(super) fn finish(&self, report: &mut CloseReport) -> Result<CloseReport, ChannelError> {
        if self.receipt.is_acknowledged() {
            return Ok(std::mem::take(report));
        }
        let Some(first) = self
            .sources
            .shutdown
            .get()
            .or_else(|| self.sources.read.get())
        else {
            return Ok(std::mem::take(report));
        };
        let canonical = self.canonical.get_or_init(|| {
            Arc::new(
                ChannelError::retained(first.clone())
                    .with_source(self.sources.clone())
                    .with_cleanup_failure_receipt(self.receipt.clone()),
            )
        });
        // A dropped observing future cannot turn a stored, unreceived error
        // into success. Only the real receiver's same-Arc ACK changes this.
        Err(ChannelError::retained(canonical.clone()))
    }
}
impl fmt::Debug for Sources {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VMessUdpTerminalSources")
            .field("shutdown", &self.shutdown.get().is_some())
            .field("read", &self.read.get().is_some())
            .finish_non_exhaustive()
    }
}
impl fmt::Display for Sources {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("same VMess UDP terminal shutdown/read sources")
    }
}
impl Error for Sources {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.shutdown
            .get()
            .or_else(|| self.read.get())
            .map(|source| source.as_ref() as _)
    }
}

#[cfg(all(test, unix))]
#[path = "../../../../tests/component/udp_close_failure.rs"]
mod tests;

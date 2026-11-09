//! This original TCP controller's pure diagnostic rows; never a Shared backref.
use super::*;
use std::{error::Error, fmt, sync::OnceLock};
#[derive(Default)]
pub(in crate::controlled_tcp) struct CloseFailures {
    sources: OnceLock<TcpCloseSources>,
    canonical: OnceLock<Arc<ChannelError>>,
}
#[derive(Clone, Default)]
pub struct TcpCloseSources(Arc<Sources>);
#[derive(Default)]
struct Sources {
    first: OnceLock<Arc<io::Error>>,
    native: OnceLock<Arc<io::Error>>,
    unknown: OnceLock<Arc<io::Error>>,
}
impl TcpCloseSources {
    pub fn first(&self) -> Option<&Arc<io::Error>> {
        self.0.first.get()
    }
    pub fn native(&self) -> Option<&Arc<io::Error>> {
        self.0.native.get()
    }
    pub fn unknown(&self) -> Option<&Arc<io::Error>> {
        self.0.unknown.get()
    }
}
impl CloseFailures {
    fn sources(&self) -> TcpCloseSources {
        self.sources.get_or_init(TcpCloseSources::default).clone()
    }
    pub(super) fn capture(&self, native: Option<Arc<io::Error>>, unknown: Option<Arc<io::Error>>) {
        if native.is_none() && unknown.is_none() {
            return;
        }
        let rows = self.sources();
        if let Some(source) = native {
            rows.0.first.get_or_init(|| source.clone());
            rows.0.native.get_or_init(|| source);
        }
        if let Some(source) = unknown {
            rows.0.first.get_or_init(|| source.clone());
            rows.0.unknown.get_or_init(|| source);
        }
    }
}
impl Shared {
    pub(super) fn unreceived_close_fault(&self) -> Option<ChannelError> {
        self.capture_close_sources();
        let source = self.close_failures.sources.get()?.first()?.clone();
        let error = self.close_fault(source);
        (!error.cleanup_failure_receipt().unwrap().is_acknowledged()).then_some(error)
    }
    pub(super) fn capture_close_sources(&self) {
        let (native, unknown) = {
            let state = self.state();
            (
                state.error.clone(),
                (!state.polling).then(|| state.unknown.clone()).flatten(),
            )
        };
        self.close_failures.capture(native, unknown);
    }
    pub(super) fn close_fault(&self, source: Arc<io::Error>) -> ChannelError {
        self.capture_close_sources();
        let rows = self.close_failures.sources();
        rows.0.first.get_or_init(|| source);
        let actual = self.close_failures.canonical.get_or_init(|| {
            let receipt = crate::contract::CleanupFailureReceipt::new();
            assert!(receipt.bind_payload(rows.0.clone()));
            let mut error = ChannelError::new(Stage::Close, Reason::Io, Impact::Session)
                .with_source(rows)
                .with_cleanup_failure_receipt(receipt);
            error.delivery = DeliveryState::Unknown;
            Arc::new(error)
        });
        ChannelError::retained(actual.clone())
    }
}
impl ControlledTcpControl {
    /// Pure same-original data, never close/accept/join authority.
    pub fn close_sources(&self) -> Option<TcpCloseSources> {
        self.shared.capture_close_sources();
        self.shared.close_failures.sources.get().cloned()
    }
}
impl fmt::Debug for TcpCloseSources {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TcpCloseSources")
            .field("native", &self.native().is_some())
            .field("unknown", &self.unknown().is_some())
            .finish()
    }
}
impl fmt::Display for TcpCloseSources {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("same original TCP close sources")
    }
}
impl Error for TcpCloseSources {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.first().map(|source| source.as_ref() as _)
    }
}
impl fmt::Debug for Sources {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TcpCloseSources")
            .field("native", &self.native.get().is_some())
            .field("unknown", &self.unknown.get().is_some())
            .finish()
    }
}
impl fmt::Display for Sources {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("original TCP failure rows")
    }
}
impl Error for Sources {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.first.get().map(|source| source.as_ref() as _)
    }
}

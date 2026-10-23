//! Keep the legacy display and the original controlled business cause together.
use zero_platform_tokio::contract::ChannelError;
#[derive(Debug)]
struct DirectFailure {
    legacy: zero_core::Error,
    native: ChannelError,
}
impl std::fmt::Display for DirectFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.legacy.fmt(formatter)
    }
}
impl std::error::Error for DirectFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.native)
    }
}
pub(super) fn retained(legacy: zero_core::Error, native: ChannelError) -> zero_engine::EngineError {
    std::io::Error::other(DirectFailure { legacy, native }).into()
}

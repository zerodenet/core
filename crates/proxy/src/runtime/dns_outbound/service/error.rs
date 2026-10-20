use std::fmt;
#[derive(Debug)]
pub(crate) struct DnsDetourFailure {
    pub(crate) outbound: String,
    pub(crate) stage: &'static str,
    pub(crate) source: zero_engine::EngineError,
}
impl fmt::Display for DnsDetourFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "DNS detour `{}` failed at {}: {}",
            self.outbound, self.stage, self.source
        )
    }
}
impl std::error::Error for DnsDetourFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

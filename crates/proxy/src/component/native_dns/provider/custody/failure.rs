use super::*;
impl Provider {
    pub(in crate::component::native_dns::provider) async fn failed_open(
        &self,
        opening: std::io::Error,
        original: &CarrierAdoption,
        end: tokio::time::Instant,
    ) -> std::io::Error {
        let cleanup = match original.retire(end).await {
            Ok(report) if report.is_complete() => {
                return zero_dns::native::NativeDnsOpenFailure::after_close(opening, original);
            }
            Ok(_) => retirement::pending_incomplete(
                "DNS failed-open original cleanup remains unconfirmed",
            ),
            Err(source) => self
                .retain_io_cleanup_failure(std::io::Error::other(source))
                .unwrap_or_else(std::convert::identity),
        };
        std::io::Error::new(cleanup.kind(), OpenCleanupFailure { opening, cleanup })
    }
}

#[derive(Debug)]
pub(in crate::component::native_dns::provider) struct OpenCleanupFailure {
    pub(in crate::component::native_dns::provider) opening: std::io::Error,
    pub(in crate::component::native_dns::provider) cleanup: std::io::Error,
}
impl std::fmt::Display for OpenCleanupFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}; original cleanup: {}", self.opening, self.cleanup)
    }
}
impl std::error::Error for OpenCleanupFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.cleanup)
    }
}

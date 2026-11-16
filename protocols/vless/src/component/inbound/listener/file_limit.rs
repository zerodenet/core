//! The existing runtime File policy configures this same cold TLS Source.
use super::*;
impl Options {
    pub fn with_tls_file_limit(
        mut self,
        limit: std::num::NonZeroUsize,
    ) -> Result<Self, zero_platform_tokio::contract::ChannelError> {
        if self.tls_material.is_empty() {
            return Ok(self);
        }
        for plan in self.tls_material.iter() {
            plan.set_file_read_limit(limit).map_err(|source| {
                zero_platform_tokio::contract::ChannelError::new(
                    zero_platform_tokio::contract::Stage::Prepare,
                    zero_platform_tokio::contract::Reason::InvalidConfig,
                    zero_platform_tokio::contract::Impact::Component,
                )
                .with_source(source)
            })?;
        }
        let mut identity = self.identity.to_vec();
        identity.extend_from_slice(b"actual-tls-file-limit-v1");
        identity.extend_from_slice(&limit.get().to_le_bytes());
        self.identity = identity.into();
        Ok(self)
    }
}

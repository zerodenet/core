//! Cold limits for this exact existing Source; allocation remains on demand.
use super::*;
impl Options {
    pub fn with_metadata_limits(
        mut self,
        probes: NonZeroUsize,
        attempts: NonZeroUsize,
        sends: NonZeroUsize,
    ) -> Result<Self, zero_transport::RuntimeError> {
        if probes.get() > 256 || attempts.get() > 4096 || sends.get() > 4096 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalid concrete metadata limits",
            )
            .into());
        }
        self.metadata_probes = probes;
        self.metadata_attempts = attempts;
        self.metadata_sends = sends;
        Ok(self)
    }
    pub fn with_quic_limits(
        mut self,
        connections: NonZeroUsize,
        users: NonZeroUsize,
    ) -> Result<Self, zero_transport::RuntimeError> {
        if connections.get() > 65536 || users.get() > 65536 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalid concrete QUIC Source limits",
            )
            .into());
        }
        self.quic_connections = connections;
        self.quic_users = users;
        Ok(self)
    }
    pub(super) fn metadata_identity(&self) -> Arc<[u8]> {
        let mut identity = self.identity.to_vec();
        identity.extend_from_slice(b"actual-source-metadata-quic-limits-v1");
        for value in [
            self.metadata_probes,
            self.metadata_attempts,
            self.metadata_sends,
            self.quic_connections,
            self.quic_users,
        ] {
            identity.extend_from_slice(&value.get().to_le_bytes());
        }
        identity.into()
    }
    pub(super) fn quic_drivers(&self) -> usize {
        self.quic_connections
            .get()
            .checked_add(1)
            .and_then(|v| {
                v.checked_add(zero_transport::finalmask::owned::driver_count(&self.masks))
            })
            .expect("finite actual Source driver composition")
    }
    pub(super) fn metadata_limits(
        &self,
    ) -> zero_platform_tokio::controlled_datagram::metadata::MetadataLimits {
        let drivers = self.quic_drivers();
        zero_platform_tokio::controlled_datagram::metadata::MetadataLimits {
            main_sockets: NonZeroUsize::new(1).unwrap(),
            probe_sockets: self.metadata_probes,
            probe_attempts_per_initialization: self.metadata_attempts,
            concurrent_send_calls: self.metadata_sends,
            write_pollers: NonZeroUsize::new(
                drivers
                    .checked_add(self.quic_users.get())
                    .and_then(|v| v.checked_add(1))
                    .expect("finite original drivers and sealed users"),
            )
            .unwrap(),
        }
    }
}

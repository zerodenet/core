use std::{io, time::Duration};

use crate::client::ClientConnection;

/// Zero's carrier policy deliberately uses bounded load-aware growth rather
/// than mirroring the official application's multiplexing-factor presets.
#[derive(Debug, Clone)]
pub struct ClientPoolPolicy {
    pub max_connections_per_identity: usize,
    pub scale_out_load_percent: usize,
    pub max_connection_age: Duration,
}

impl Default for ClientPoolPolicy {
    fn default() -> Self {
        Self {
            max_connections_per_identity: 4,
            scale_out_load_percent: 75,
            max_connection_age: Duration::from_secs(30 * 60),
        }
    }
}

impl ClientPoolPolicy {
    pub(super) fn validate(&self) -> io::Result<()> {
        if self.max_connections_per_identity == 0
            || self.scale_out_load_percent == 0
            || self.scale_out_load_percent > 100
            || self.max_connection_age.is_zero()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid mieru client pool policy",
            ));
        }
        Ok(())
    }

    pub(super) fn scale_out_load(&self, connection: &ClientConnection) -> usize {
        connection
            .max_streams()
            .saturating_mul(self.scale_out_load_percent)
            .div_ceil(100)
            .max(1)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ClientPoolSnapshot {
    pub identities: usize,
    pub connections: usize,
    pub active_streams: usize,
    pub pending_opens: usize,
    pub dialing_identities: usize,
}

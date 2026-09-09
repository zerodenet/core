//! Neutral QUIC transport parameters. Protocols supply congestion policy.
use std::{io, sync::Arc, time::Duration};

#[derive(Debug, Clone, Copy)]
pub struct QuicTransportOptions {
    pub stream_receive_window: u64,
    pub connection_receive_window: u64,
    pub max_stream_receive_window: Option<u64>,
    pub max_connection_receive_window: Option<u64>,
    pub send_window: u64,
    /// Per-connection pacing ceiling in bytes/sec; zero or None is unlimited.
    pub max_send_rate: Option<u64>,
    pub max_idle_timeout: Duration,
    pub keep_alive_interval: Option<Duration>,
    pub max_incoming_streams: u32,
    pub disable_path_mtu_discovery: bool,
}
impl QuicTransportOptions {
    pub fn build(
        self,
        controller: Arc<dyn quinn::congestion::ControllerFactory + Send + Sync>,
    ) -> Result<quinn::TransportConfig, io::Error> {
        let mut config = quinn::TransportConfig::default();
        config.stream_receive_window(
            self.stream_receive_window
                .try_into()
                .map_err(io::Error::other)?,
        );
        config.receive_window(
            self.connection_receive_window
                .try_into()
                .map_err(io::Error::other)?,
        );
        let stream_max = self
            .max_stream_receive_window
            .unwrap_or(self.stream_receive_window);
        let connection_max = self
            .max_connection_receive_window
            .unwrap_or(self.connection_receive_window);
        if self.stream_receive_window == 0
            || self.connection_receive_window == 0
            || stream_max < self.stream_receive_window
            || connection_max < self.connection_receive_window
            || stream_max > (1 << 60)
            || connection_max > (1 << 60)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid QUIC receive-window bounds",
            ));
        }
        if stream_max > self.stream_receive_window
            || connection_max > self.connection_receive_window
        {
            config.receive_window_controller(Arc::new(super::receive_window::Factory {
                stream_max,
                connection_max,
            }));
        }
        config.send_window(self.send_window);
        config.max_idle_timeout(Some(
            self.max_idle_timeout.try_into().map_err(io::Error::other)?,
        ));
        config.keep_alive_interval(self.keep_alive_interval);
        config.max_concurrent_bidi_streams(self.max_incoming_streams.into());
        config.datagram_receive_buffer_size(Some(1024 * 1024));
        if self.disable_path_mtu_discovery {
            config.mtu_discovery_config(None);
        }
        config.congestion_controller_factory(super::rate_limit::cap_factory(
            controller,
            self.max_send_rate,
        ));
        Ok(config)
    }
}

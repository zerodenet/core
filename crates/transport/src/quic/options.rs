//! Neutral QUIC transport parameters. Protocols supply congestion policy.
use std::{io, sync::Arc, time::Duration};

#[derive(Debug, Clone, Copy)]
pub struct QuicTransportOptions {
    pub stream_receive_window: u64,
    pub connection_receive_window: u64,
    pub send_window: u64,
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
        config.congestion_controller_factory(controller);
        Ok(config)
    }
}

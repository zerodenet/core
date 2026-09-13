//! Protocol congestion selection; the QUIC carrier executes shared controllers.
use crate::settings::{Congestion, Settings};
use quinn::congestion::{ControllerFactory, NewRenoConfig};
use std::{sync::Arc, time::Duration};
use zero_transport::quic::bbr::BbrConfig;
mod bbr;
pub(super) use zero_transport::quic::negotiated::negotiate;
pub(super) fn factory(settings: Settings) -> Arc<dyn ControllerFactory + Send + Sync> {
    let adaptive: Arc<dyn ControllerFactory + Send + Sync> = match settings.congestion {
        Congestion::Bbr => Arc::new(BbrConfig {
            parameters: bbr::parameters(settings.bbr_profile),
            initial_window: settings.bbr_initial_window,
        }),
        Congestion::Reno => Arc::new(NewRenoConfig::default()),
    };
    Arc::new(zero_transport::quic::negotiated::Factory {
        adaptive,
        disable_loss_compensation: settings.disable_loss_compensation,
        minimum_packets: 2,
    })
}
pub(super) fn transport(settings: Settings) -> Result<quinn::TransportConfig, std::io::Error> {
    settings
        .validate()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
    let q = settings.quic;
    zero_transport::quic::QuicTransportOptions {
        stream_receive_window: q.stream_receive_window,
        connection_receive_window: q.connection_receive_window,
        max_stream_receive_window: q.max_stream_receive_window,
        max_connection_receive_window: q.max_connection_receive_window,
        send_window: q.send_window,
        // Negotiated bandwidth is the Brutal target, not a hard pacing ceiling:
        // loss compensation may send above it, and auto must remain adaptive.
        max_send_rate: None,
        max_idle_timeout: Duration::from_secs(q.max_idle_timeout_secs),
        keep_alive_interval: (q.keep_alive_interval_secs != 0)
            .then(|| Duration::from_secs(q.keep_alive_interval_secs)),
        max_incoming_streams: q.max_incoming_streams,
        disable_path_mtu_discovery: q.disable_path_mtu_discovery,
    }
    .build(factory(settings))
}

//! Xray v26.3.27 Hysteria carrier policy, separate from QUIC machinery.
use crate::quic::{bbr::BbrConfig, negotiated, QuicTransportOptions};
use std::{io, sync::Arc, time::Duration};
#[derive(Debug, Clone, Copy, Default)]
pub struct OptionsRef<'a> {
    pub auth: &'a str,
    pub congestion: &'a str,
    pub uplink_bytes_per_sec: u64,
    pub downlink_bytes_per_sec: u64,
    pub quic: Parameters,
    pub udp_hop: Option<crate::datagram_hop::OptionsRef<'a>>,
}
#[derive(Debug, Clone, Copy, Default)]
pub struct Parameters {
    pub initial_stream_receive_window: u64,
    pub max_stream_receive_window: u64,
    pub initial_connection_receive_window: u64,
    pub max_connection_receive_window: u64,
    pub max_idle_timeout_secs: u64,
    pub keep_alive_secs: u64,
    pub max_incoming_streams: u32,
    pub disable_path_mtu_discovery: bool,
}
#[derive(Debug, Clone, Copy, Default)]
pub enum Congestion {
    Reno,
    Bbr,
    #[default]
    Brutal,
    ForceBrutal,
}
impl Congestion {
    pub fn parse(value: &str) -> io::Result<Self> {
        match value {
            "reno" => Ok(Self::Reno),
            "bbr" => Ok(Self::Bbr),
            "" | "brutal" => Ok(Self::Brutal),
            "force-brutal" => Ok(Self::ForceBrutal),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "unknown Hysteria congestion controller",
            )),
        }
    }
}
impl super::Profile {
    pub fn from_options(options: OptionsRef<'_>) -> io::Result<Self> {
        let mut profile = Self::new(options.auth)?;
        profile.congestion = Congestion::parse(options.congestion)?;
        profile.uplink = options.uplink_bytes_per_sec;
        profile.downlink = options.downlink_bytes_per_sec;
        profile.quic = options.quic;
        profile.hopping = options
            .udp_hop
            .map(crate::datagram_hop::Profile::new)
            .transpose()?;
        if matches!(profile.congestion, Congestion::ForceBrutal) && profile.uplink == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "force-brutal requires uplink bandwidth",
            ));
        }
        profile.transport(false)?;
        Ok(profile)
    }
    pub fn transport(&self, server: bool) -> io::Result<quinn::TransportConfig> {
        self.quic.transport(server, self.congestion)
    }
    pub(super) fn negotiate(&self, connection: &quinn::Connection, peer_downlink: u64) {
        let rate = match self.congestion {
            Congestion::Brutal => self.uplink.min(peer_downlink),
            Congestion::ForceBrutal => self.uplink,
            Congestion::Reno | Congestion::Bbr => 0,
        };
        negotiated::negotiate(connection, rate);
    }
}
impl Parameters {
    pub fn transport(
        self,
        server: bool,
        congestion: Congestion,
    ) -> io::Result<quinn::TransportConfig> {
        if (self.max_idle_timeout_secs != 0 && !(4..=120).contains(&self.max_idle_timeout_secs))
            || (self.keep_alive_secs != 0 && !(2..=60).contains(&self.keep_alive_secs))
            || (self.max_incoming_streams != 0 && self.max_incoming_streams < 8)
            || [
                self.initial_stream_receive_window,
                self.max_stream_receive_window,
                self.initial_connection_receive_window,
                self.max_connection_receive_window,
            ]
            .iter()
            .any(|value| *value != 0 && *value < 16384)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid Hysteria QUIC parameter range",
            ));
        }
        fn default(value: u64, fallback: u64) -> u64 {
            if value == 0 {
                fallback
            } else {
                value
            }
        }
        let adaptive: Arc<dyn quinn::congestion::ControllerFactory + Send + Sync> = match congestion
        {
            Congestion::Reno => Arc::new(quinn::congestion::NewRenoConfig::default()),
            _ => Arc::new(BbrConfig::default()),
        };
        QuicTransportOptions {
            stream_receive_window: default(self.initial_stream_receive_window, 8_388_608),
            max_stream_receive_window: Some(default(self.max_stream_receive_window, 8_388_608)),
            connection_receive_window: default(self.initial_connection_receive_window, 20_971_520),
            max_connection_receive_window: Some(default(
                self.max_connection_receive_window,
                20_971_520,
            )),
            send_window: 64 * 1024 * 1024,
            max_send_rate: None,
            max_idle_timeout: Duration::from_secs(default(self.max_idle_timeout_secs, 30)),
            keep_alive_interval: (!server && self.keep_alive_secs > 0)
                .then(|| Duration::from_secs(self.keep_alive_secs)),
            max_incoming_streams: if self.max_incoming_streams == 0 {
                1024
            } else {
                self.max_incoming_streams
            },
            disable_path_mtu_discovery: self.disable_path_mtu_discovery,
        }
        .build(Arc::new(negotiated::Factory {
            adaptive,
            disable_loss_compensation: false,
            minimum_packets: 1,
        }))
    }
}

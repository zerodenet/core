//! Runtime-independent Hysteria2 transport policy and configuration validation.
use alloc::string::String;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Congestion {
    Bbr,
    Reno,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Settings {
    pub upload: u64,
    pub download: u64,
    pub ignore_client_bandwidth: bool,
    pub disable_loss_compensation: bool,
    pub congestion: Congestion,
    pub bbr_initial_window: u64,
    pub quic: QuicSettings,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct QuicSettings {
    pub stream_receive_window: u64,
    pub connection_receive_window: u64,
    pub send_window: u64,
    pub max_idle_timeout_secs: u64,
    pub keep_alive_interval_secs: u64,
    pub max_incoming_streams: u32,
    pub disable_path_mtu_discovery: bool,
}

impl Default for QuicSettings {
    fn default() -> Self {
        Self {
            stream_receive_window: 8_388_608,
            connection_receive_window: 20_971_520,
            send_window: 20_971_520,
            max_idle_timeout_secs: 30,
            keep_alive_interval_secs: 10,
            max_incoming_streams: 1024,
            disable_path_mtu_discovery: false,
        }
    }
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            upload: 0,
            download: 0,
            ignore_client_bandwidth: false,
            disable_loss_compensation: false,
            congestion: Congestion::Bbr,
            bbr_initial_window: 32 * 1200,
            quic: QuicSettings::default(),
        }
    }
}
impl Settings {
    pub fn validate(&self) -> Result<(), &'static str> {
        for value in [self.upload, self.download] {
            if value != 0 && !(65_536..=u64::MAX / 8).contains(&value) {
                return Err(
                    "bandwidth must be zero or at least 65536 bytes/sec and fit a bit rate",
                );
            }
        }
        for value in [
            self.quic.stream_receive_window,
            self.quic.connection_receive_window,
            self.quic.send_window,
        ] {
            if !(16_384..=(1u64 << 60)).contains(&value) {
                return Err("QUIC windows must be between 16384 and 2^60 bytes");
            }
        }
        if !(4..=120).contains(&self.quic.max_idle_timeout_secs) {
            return Err("QUIC idle timeout must be between 4 and 120 seconds");
        }
        if self.quic.keep_alive_interval_secs != 0
            && !(2..=60).contains(&self.quic.keep_alive_interval_secs)
        {
            return Err("QUIC keep-alive must be zero or between 2 and 60 seconds");
        }
        if self.quic.keep_alive_interval_secs >= self.quic.max_idle_timeout_secs {
            return Err("QUIC keep-alive must be shorter than idle timeout");
        }
        if self.quic.max_incoming_streams < 8 {
            return Err("QUIC maximum incoming streams must be at least 8");
        }
        if !(4 * 1200..=16_777_216).contains(&self.bbr_initial_window) {
            return Err("BBR initial window must be between 4800 and 16777216 bytes");
        }
        Ok(())
    }

    /// Official client policy: auto forces adaptive CC; otherwise take the
    /// smallest nonzero local upload and server receive limit.
    pub fn client_send_rate(&self, peer: crate::handshake::ReceiveBandwidth) -> u64 {
        match peer {
            crate::handshake::ReceiveBandwidth::Auto => 0,
            crate::handshake::ReceiveBandwidth::Limit(peer) => bounded_rate(self.upload, peer),
        }
    }
    pub fn server_send_rate(&self, client_receive: u64) -> u64 {
        if self.ignore_client_bandwidth || client_receive == 0 {
            0
        } else {
            bounded_rate(self.upload, client_receive)
        }
    }
}
fn bounded_rate(local: u64, peer: u64) -> u64 {
    match (local, peer) {
        (0, _) => peer,
        (_, 0) => local,
        _ => local.min(peer),
    }
}

/// Hysteria bandwidth strings are decimal bit rates, converted to bytes/sec.
pub fn parse_bandwidth(value: Option<&str>) -> Result<u64, &'static str> {
    let Some(value) = value else {
        return Ok(0);
    };
    let value = value.trim();
    let split = value
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(value.len());
    let number: u64 = value[..split]
        .parse()
        .map_err(|_| "invalid bandwidth number")?;
    let unit: String = value[split..].trim().to_ascii_lowercase();
    let multiplier = match unit.as_str() {
        "b" | "bps" => 1,
        "k" | "kb" | "kbps" => 1000,
        "m" | "mb" | "mbps" => 1_000_000,
        "g" | "gb" | "gbps" => 1_000_000_000,
        "t" | "tb" | "tbps" => 1_000_000_000_000,
        _ => return Err("bandwidth requires bps, Kbps, Mbps, Gbps or Tbps"),
    };
    number
        .checked_mul(multiplier)
        .map(|bits| bits / 8)
        .ok_or("bandwidth overflow")
}

#[cfg(feature = "validation")]
pub fn validate_proxy_url(value: &str) -> Result<url::Url, &'static str> {
    let url = url::Url::parse(value).map_err(|_| "invalid masquerade proxy URL")?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(
            "masquerade proxy requires an absolute HTTP/HTTPS URL without user info or fragment",
        );
    }
    Ok(url)
}
pub fn validate_masquerade_response(status: u16, content_type: &str) -> Result<(), &'static str> {
    if !(200..=599).contains(&status) {
        return Err("masquerade status must be between 200 and 599");
    }
    if content_type.is_empty()
        || content_type
            .bytes()
            .any(|b| b < 32 || b == 127 || !b.is_ascii())
    {
        return Err("invalid masquerade content type");
    }
    Ok(())
}
/// Percent decode first, then reject traversal and platform-specific separators.
pub fn decode_site_path(value: &str) -> Result<String, &'static str> {
    let mut bytes = alloc::vec::Vec::new();
    let mut input = value.bytes();
    while let Some(byte) = input.next() {
        if byte == b'%' {
            let digit = |b: u8| {
                (b as char)
                    .to_digit(16)
                    .map(|v| v as u8)
                    .ok_or("invalid path escape")
            };
            bytes.push(
                digit(input.next().ok_or("truncated path escape")?)? * 16
                    + digit(input.next().ok_or("truncated path escape")?)?,
            );
        } else {
            bytes.push(byte);
        }
    }
    let path = String::from_utf8(bytes).map_err(|_| "invalid path UTF-8")?;
    if path.contains(['\\', ':', '\0']) || path.split('/').any(|p| p == "..") {
        return Err("invalid site path");
    }
    Ok(path.trim_start_matches('/').into())
}

impl Congestion {
    pub fn parse(value: &str) -> Result<Self, &'static str> {
        match value.to_ascii_lowercase().as_str() {
            "" | "bbr" => Ok(Self::Bbr),
            "reno" => Ok(Self::Reno),
            _ => Err("hysteria2 congestion type must be bbr or reno"),
        }
    }
}

/// Parsed XHTTP framing mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum XhttpMode {
    Auto,
    PacketUp,
    StreamUp,
    StreamOne,
}

impl XhttpMode {
    pub fn parse(s: &str) -> Self {
        match s {
            "" | "auto" => XhttpMode::Auto,
            "packet-up" => XhttpMode::PacketUp,
            "stream-up" => XhttpMode::StreamUp,
            "stream-one" => XhttpMode::StreamOne,
            _ => XhttpMode::Auto,
        }
    }

    /// The current profile has no separate download settings: official auto
    /// chooses stream-one with REALITY and packet-up otherwise.
    pub fn resolve(self, reality: bool) -> Self {
        match self {
            Self::Auto if reality => Self::StreamOne,
            Self::Auto => Self::PacketUp,
            mode => mode,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::PacketUp => "packet-up",
            Self::StreamUp => "stream-up",
            Self::StreamOne => "stream-one",
        }
    }
    pub fn is_single_connection(self) -> bool {
        matches!(self, XhttpMode::StreamOne)
    }
}

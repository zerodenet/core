use std::fmt;
use std::str::FromStr;

/// Versioned ClientHello templates supported by ztls.
///
/// The short aliases are intentionally mapped to a concrete template so a
/// configuration does not silently change its wire image when a browser ships.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ClientHelloProfile {
    Chrome120,
    Firefox120,
    Safari160,
    Edge120,
}

impl ClientHelloProfile {
    pub const DEFAULT: Self = Self::Chrome120;

    pub fn canonical_name(self) -> &'static str {
        match self {
            Self::Chrome120 => "chrome-120",
            Self::Firefox120 => "firefox-120",
            Self::Safari160 => "safari-16.0",
            Self::Edge120 => "edge-120",
        }
    }

    pub fn alpn_protocols(self) -> &'static [&'static str] {
        match self {
            Self::Safari160 => &["h2", "http/1.1"],
            Self::Chrome120 | Self::Firefox120 | Self::Edge120 => &["h2", "http/1.1"],
        }
    }

    pub fn cipher_suites(self) -> &'static [u16] {
        match self {
            Self::Chrome120 | Self::Edge120 => &[0x1301, 0x1302, 0x1303],
            Self::Firefox120 => &[0x1301, 0x1303, 0x1302],
            Self::Safari160 => &[0x1301, 0x1302, 0x1303],
        }
    }

    pub(crate) fn supported_groups(self) -> &'static [u16] {
        match self {
            Self::Safari160 => &[0x0017, 0x001d, 0x0018],
            Self::Chrome120 | Self::Firefox120 | Self::Edge120 => &[0x001d, 0x0017, 0x0018],
        }
    }

    pub(crate) const fn key_share_group(self) -> u16 {
        // ztls currently implements X25519 key agreement for every profile.
        0x001d
    }

    /// Extension order excluding padding. Only extensions implemented by ztls
    /// are emitted; the order is kept distinct per browser family.
    pub(crate) fn extension_order(self) -> &'static [u16] {
        match self {
            Self::Chrome120 | Self::Edge120 => &[
                0x0000, 0x0017, 0x000b, 0x000a, 0x0033, 0x000d, 0x0032, 0x0010, 0x001b, 0x0016,
                0x002d, 0x002b,
            ],
            Self::Firefox120 => &[
                0x0000, 0x0017, 0x000a, 0x000b, 0x000d, 0x0032, 0x0010, 0x002b, 0x0033, 0x002d,
                0x001b, 0x0016,
            ],
            Self::Safari160 => &[
                0x0000, 0x000b, 0x000a, 0x000d, 0x0032, 0x0017, 0x0010, 0x002b, 0x0033, 0x002d,
                0x001b, 0x0016,
            ],
        }
    }
}

impl Default for ClientHelloProfile {
    fn default() -> Self {
        Self::DEFAULT
    }
}

impl fmt::Display for ClientHelloProfile {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.canonical_name())
    }
}

impl FromStr for ClientHelloProfile {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "chrome" | "chrome-120" => Ok(Self::Chrome120),
            "firefox" | "firefox-120" => Ok(Self::Firefox120),
            "safari" | "safari-16" | "safari-16.0" => Ok(Self::Safari160),
            "edge" | "edge-120" => Ok(Self::Edge120),
            _ => Err(format!(
                "unsupported client fingerprint `{value}`; expected chrome, firefox, safari, or edge"
            )),
        }
    }
}

// SPDX-License-Identifier: MPL-2.0
// VLESS Encryption wire behavior adapted from XTLS/Xray-core v26.3.27
// (d2758a023cd7f4174a5a5fa4ff66e487d4342ba0), proxy/vless/encryption.
use alloc::vec::Vec;
use base64::Engine;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mask {
    Native,
    XorPublic,
    Random,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PaddingRange {
    pub probability: u32,
    pub min: u32,
    pub max: u32,
}

/// Validated protocol-owned settings. Debug intentionally omits key material.
#[derive(Clone)]
pub struct EncryptionConfig {
    pub mask: Mask,
    pub resume: bool,
    pub seconds: (u16, u16),
    pub padding: Vec<PaddingRange>,
    pub(crate) keys: Vec<Vec<u8>>,
}
impl core::fmt::Debug for EncryptionConfig {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("EncryptionConfig")
            .field("mask", &self.mask)
            .field("resume", &self.resume)
            .field("seconds", &self.seconds)
            .field("padding", &self.padding)
            .field("key_count", &self.keys.len())
            .finish()
    }
}
impl EncryptionConfig {
    pub fn client(value: &str) -> Result<Option<Self>, &'static str> {
        Self::parse(value, false)
    }
    pub fn server(value: &str) -> Result<Option<Self>, &'static str> {
        Self::parse(value, true)
    }

    fn parse(value: &str, server: bool) -> Result<Option<Self>, &'static str> {
        if value == "none" {
            return Ok(None);
        }
        let mut parts = value.split('.');
        if parts.next() != Some("mlkem768x25519plus") {
            return Err("unsupported VLESS encryption scheme");
        }
        let mask = match parts.next() {
            Some("native") => Mask::Native,
            Some("xorpub") => Mask::XorPublic,
            Some("random") => Mask::Random,
            _ => return Err("invalid VLESS encryption mask"),
        };
        let mode = parts
            .next()
            .ok_or("missing encryption lifetime or RTT mode")?;
        let (resume, seconds) = if server {
            let mut range = mode.trim_end_matches('s').split('-');
            let low: u16 = range
                .next()
                .ok_or("missing ticket lifetime")?
                .parse()
                .map_err(|_| "ticket lifetime must fit 0..=65535 seconds")?;
            let high: u16 = range
                .next()
                .map(|s| s.parse())
                .transpose()
                .map_err(|_| "invalid ticket lifetime range")?
                .unwrap_or(0);
            if range.next().is_some() {
                return Err("invalid ticket lifetime range");
            }
            (low > 0 || high > 0, (low, high))
        } else {
            match mode {
                "1rtt" => (false, (0, 0)),
                "0rtt" => (true, (0, 0)),
                _ => return Err("encryption requires 1rtt or 0rtt"),
            }
        };
        let mut padding = Vec::new();
        let mut keys = Vec::new();
        for part in parts {
            if part.len() < 20 {
                if !keys.is_empty() {
                    return Err("padding must precede encryption keys");
                }
                let numbers: Vec<_> = part.split('-').collect();
                if numbers.len() != 3 {
                    return Err("padding requires probability-min-max");
                }
                let parse = |s: &str| s.parse::<u32>().map_err(|_| "invalid padding range");
                padding.push(PaddingRange {
                    probability: parse(numbers[0])?,
                    min: parse(numbers[1])?,
                    max: parse(numbers[2])?,
                });
            } else {
                let key = base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .decode(part)
                    .map_err(|_| "invalid encryption key encoding")?;
                if key.len() != 32 && key.len() != if server { 64 } else { 1184 } {
                    return Err("invalid encryption key length");
                }
                keys.push(key);
            }
        }
        if keys.is_empty() {
            return Err("encryption requires at least one key");
        }
        if let Some(first) = padding.first() {
            if first.probability < 100 || first.min < 35 || first.max < 35 {
                return Err("first padding length must be at least 35");
            }
        }
        let total: u64 = padding
            .iter()
            .step_by(2)
            .map(|r| u64::from(r.min.max(r.max)))
            .sum();
        if total > 65553 {
            return Err("total padding exceeds 65553 bytes");
        }
        Ok(Some(Self {
            mask,
            resume,
            seconds,
            padding,
            keys,
        }))
    }
}

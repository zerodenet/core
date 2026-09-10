//! Runtime traffic shaping compatible with the official Mieru v3.33.0 profile.

mod padding;
mod poll_write;

use alloc::{format, string::String, vec::Vec};
use mieru_config::{MieruNoncePatternConfig, MieruNonceType, MieruTrafficPatternConfig};
use rand::Rng;
use sha2::Digest;
use tokio::io::{self, AsyncWrite, AsyncWriteExt};
use zero_core::Error;
use zero_traits::AsyncSocket;

use crate::crypto::{NonceConfig, NoncePattern};

pub use padding::{data_padding_lengths, session_padding};
pub(crate) use poll_write::FragmentedWriteState;

const OFFICIAL_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Effective TCP write fragmentation settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpFragmentPattern {
    pub enable: bool,
    pub max_sleep_ms: u16,
}

/// Effective nonce settings. The applied state for UDP is owned by the packet
/// codec because the official setting can apply to the first packet only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NoncePatternRuntime {
    pub pattern: NoncePattern,
    pub apply_to_all_udp_packet: bool,
}

/// Fully materialized traffic pattern. Missing fields are deterministically
/// generated from the profile seed just like official Mieru v3.33.0.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrafficPattern {
    pub tcp_fragment: TcpFragmentPattern,
    pub nonce: NoncePatternRuntime,
}

impl TrafficPattern {
    pub fn from_config(config: Option<&MieruTrafficPatternConfig>) -> Result<Self, Error> {
        let empty = MieruTrafficPatternConfig::default();
        let config = config.unwrap_or(&empty);
        config
            .validate()
            .map_err(|_| Error::Config("invalid mieru traffic pattern"))?;

        let seed = config.seed.unwrap_or_else(implicit_host_seed);
        let unlock_all = config.unlock_all.unwrap_or(false);
        let tcp = config.tcp_fragment.as_ref();
        let enable = tcp.and_then(|item| item.enable).unwrap_or_else(|| {
            unlock_all && fixed_int(2, &format!("{seed}:tcpFragment.enable")) == 1
        });
        let max_sleep_ms = tcp.and_then(|item| item.max_sleep_ms).unwrap_or_else(|| {
            if unlock_all {
                fixed_int(100, &format!("{seed}:tcpFragment.maxSleepMs")) as u16 + 1
            } else {
                0
            }
        });

        let nonce = effective_nonce(config.nonce.as_ref(), seed, unlock_all)?;
        Ok(Self {
            tcp_fragment: TcpFragmentPattern {
                enable,
                max_sleep_ms,
            },
            nonce,
        })
    }

    /// Builds the nonce configuration for one cipher direction. Packet codecs
    /// pass `apply_pattern = false` after the first packet when the effective
    /// profile does not enable `apply_to_all_udp_packet`.
    pub fn nonce_config(&self, username: &str, apply_pattern: bool) -> NonceConfig {
        NonceConfig {
            pattern: if apply_pattern {
                self.nonce.pattern.clone()
            } else {
                NoncePattern::Random
            },
            username: (!username.is_empty()).then(|| username.into()),
        }
    }
}

fn effective_nonce(
    original: Option<&MieruNoncePatternConfig>,
    seed: i32,
    unlock_all: bool,
) -> Result<NoncePatternRuntime, Error> {
    let kind = original.and_then(|item| item.kind).unwrap_or_else(|| {
        if unlock_all {
            match fixed_int(3, &format!("{seed}:nonce.type")) {
                0 => MieruNonceType::Random,
                1 => MieruNonceType::Printable,
                _ => MieruNonceType::PrintableSubset,
            }
        } else if fixed_int(2, &format!("{seed}:nonce.type")) == 0 {
            MieruNonceType::Printable
        } else {
            MieruNonceType::PrintableSubset
        }
    });
    let apply_to_all_udp_packet = original
        .and_then(|item| item.apply_to_all_udp_packet)
        .unwrap_or_else(|| fixed_int(2, &format!("{seed}:nonce.applyToAllUDPPacket")) == 1);
    let min_len = original.and_then(|item| item.min_len).unwrap_or_else(|| {
        if unlock_all {
            fixed_int(13, &format!("{seed}:nonce.minLen")) as u8
        } else {
            fixed_int(7, &format!("{seed}:nonce.minLen")) as u8 + 6
        }
    });
    let max_len = original.and_then(|item| item.max_len).unwrap_or_else(|| {
        min_len + fixed_int(13 - min_len as usize, &format!("{seed}:nonce.maxLen")) as u8
    });
    // Official validation runs before implicit generation. If only max_len is
    // explicit, generated min_len can exceed it; the cipher then clamps min to
    // max when selecting the rewrite length. Normalize that applied behavior.
    let min_len = min_len.min(max_len);
    let pattern = match kind {
        MieruNonceType::Random => NoncePattern::Random,
        MieruNonceType::Printable => NoncePattern::Printable {
            min_len: min_len as usize,
            max_len: max_len as usize,
        },
        MieruNonceType::PrintableSubset => NoncePattern::PrintableSubset {
            min_len: min_len as usize,
            max_len: max_len as usize,
        },
        MieruNonceType::Fixed => NoncePattern::Fixed {
            prefixes: original
                .into_iter()
                .flat_map(|item| &item.custom_hex_strings)
                .map(|value| decode_hex(value))
                .collect::<Result<Vec<_>, _>>()?,
        },
    };
    Ok(NoncePatternRuntime {
        pattern,
        apply_to_all_udp_packet,
    })
}

fn decode_hex(value: &str) -> Result<Vec<u8>, Error> {
    let bytes = value.as_bytes();
    if !bytes.len().is_multiple_of(2) {
        return Err(Error::Config("invalid mieru nonce prefix"));
    }
    bytes
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            let high = hex_digit(pair[0]).ok_or(Error::Config("invalid mieru nonce prefix"))?;
            let low = hex_digit(pair[1]).ok_or(Error::Config("invalid mieru nonce prefix"))?;
            Ok(high << 4 | low)
        })
        .collect()
}

fn hex_digit(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

/// Writes one encrypted Mieru segment, optionally split into extra TCP writes.
/// Fragment sizes and sleeps follow the official v3.33.0 algorithm.
pub async fn write_with_fragmentation<W: AsyncWrite + Unpin>(
    writer: &mut W,
    data: &[u8],
    fragment: TcpFragmentPattern,
) -> io::Result<()> {
    if !fragment.enable {
        return writer.write_all(data).await;
    }
    let mut remaining = data;
    let min_len = (data.len() as f64).sqrt() as usize + 1;
    let max_len = min_len.max(data.len() / 2);
    while !remaining.is_empty() {
        let len = rand::thread_rng()
            .gen_range(min_len..=max_len)
            .min(remaining.len());
        writer.write_all(&remaining[..len]).await?;
        if fragment.max_sleep_ms > 0 {
            let sleep_ms = rand::thread_rng().gen_range(0..=fragment.max_sleep_ms);
            tokio::time::sleep(core::time::Duration::from_millis(sleep_ms.into())).await;
        }
        remaining = &remaining[len..];
    }
    Ok(())
}

/// `AsyncSocket` counterpart used by the protocol handshake surface, whose
/// runtime-neutral contract intentionally does not require Tokio `AsyncWrite`.
pub async fn write_socket_with_fragmentation<S: AsyncSocket>(
    socket: &mut S,
    data: &[u8],
    fragment: TcpFragmentPattern,
) -> Result<(), S::Error> {
    if !fragment.enable {
        return socket.write_all(data).await;
    }
    let mut remaining = data;
    let min_len = (data.len() as f64).sqrt() as usize + 1;
    let max_len = min_len.max(data.len() / 2);
    while !remaining.is_empty() {
        let len = rand::thread_rng()
            .gen_range(min_len..=max_len)
            .min(remaining.len());
        socket.write_all(&remaining[..len]).await?;
        if fragment.max_sleep_ms > 0 {
            let sleep_ms = rand::thread_rng().gen_range(0..=fragment.max_sleep_ms);
            tokio::time::sleep(core::time::Duration::from_millis(sleep_ms.into())).await;
        }
        remaining = &remaining[len..];
    }
    Ok(())
}

pub(super) fn fixed_int(n: usize, hint: &str) -> usize {
    if n == 0 {
        return 0;
    }
    let mut digest = sha2::Sha256::digest(hint.as_bytes());
    digest[0] &= 0x7f;
    u32::from_be_bytes(digest[..4].try_into().expect("SHA-256 prefix")) as usize % n
}

pub(super) fn host_version_fixed_int(n: usize) -> usize {
    fixed_int(n, &format!("{} {OFFICIAL_VERSION}", system_hostname()))
}

fn implicit_host_seed() -> i32 {
    host_version_fixed_int(i32::MAX as usize) as i32
}

fn system_hostname() -> String {
    ["HOSTNAME", "HOST", "COMPUTERNAME"]
        .into_iter()
        .find_map(|key| std::env::var(key).ok().filter(|value| !value.is_empty()))
        .or_else(|| {
            std::fs::read_to_string("/etc/hostname")
                .ok()
                .map(|value| value.trim().into())
                .filter(|value: &String| !value.is_empty())
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests;

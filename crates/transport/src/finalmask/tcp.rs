//! TCP masks wrap the raw carrier before TLS and application handshakes.
use crate::TcpRelayStream;
use rand::Rng;
use std::{io, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
mod custom;
mod fragment;
mod prepared;
mod stream;
pub use prepared::{wrap_prepared, PreparedMasks};
#[derive(Debug, Clone)]
pub enum Mask {
    Custom(Custom),
    Fragment(Fragment),
    Sudoku(super::sudoku::Settings),
}
#[derive(Debug, Clone, Default)]
pub struct Custom {
    pub clients: Vec<Vec<Item>>,
    pub servers: Vec<Vec<Item>>,
    pub errors: Vec<Vec<Item>>,
}
#[derive(Debug, Clone)]
pub struct Item {
    pub delay_ms: Range,
    pub content: super::udp::Item,
}
#[derive(Debug, Clone, Copy, Default)]
pub struct Range {
    pub minimum: u64,
    pub maximum: u64,
}
impl Range {
    pub(in crate::finalmask) fn sample(self) -> u64 {
        rand::rng().random_range(self.minimum..=self.maximum)
    }
    pub(in crate::finalmask) fn validate(self) -> io::Result<()> {
        if self.minimum > self.maximum {
            Err(invalid("invalid FinalMask range"))
        } else {
            Ok(())
        }
    }
}
#[derive(Debug, Clone)]
pub struct Fragment {
    pub packets: Range,
    pub length: Range,
    pub delay_ms: Range,
    pub max_splits: Range,
}
impl Mask {
    pub(super) fn validate(&self) -> io::Result<()> {
        match self {
            Self::Fragment(config) => {
                config.packets.validate()?;
                config.length.validate()?;
                config.delay_ms.validate()?;
                config.max_splits.validate()?;
                if config.length.minimum == 0 || config.length.maximum > 65535 {
                    return Err(invalid("invalid TCP fragment length"));
                }
            }
            Self::Custom(config) => {
                for sequences in [&config.clients, &config.servers, &config.errors] {
                    if sequences.len() > 1024 {
                        return Err(invalid("too many custom TCP sequences"));
                    }
                    for sequence in sequences {
                        let mut size = 0usize;
                        for item in sequence {
                            item.delay_ms.validate()?;
                            size = size
                                .checked_add(item.content.length())
                                .ok_or_else(|| invalid("TCP custom header too large"))?;
                            if let super::udp::Item::Random {
                                minimum, maximum, ..
                            } = &item.content
                            {
                                if minimum > maximum {
                                    return Err(invalid("invalid custom TCP random range"));
                                }
                            }
                        }
                        if size > 1024 * 1024 {
                            return Err(invalid("TCP custom sequence too large"));
                        }
                    }
                }
            }
            Self::Sudoku(settings) => {
                super::sudoku::Profile::new(settings)?;
            }
        }
        Ok(())
    }
}
pub async fn wrap(
    stream: TcpRelayStream,
    masks: &[Mask],
    server: bool,
) -> io::Result<TcpRelayStream> {
    wrap_prepared(stream, &PreparedMasks::new(masks)?, server).await
}
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
#[path = "../../tests/finalmask/tcp.rs"]
mod tests;

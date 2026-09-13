// SPDX-License-Identifier: MPL-2.0
// VLESS Encryption wire behavior adapted from XTLS/Xray-core v26.3.27
// (d2758a023cd7f4174a5a5fa4ff66e487d4342ba0), proxy/vless/encryption.
use super::{
    config::PaddingRange,
    crypto::{invalid, AeadState},
};
use rand::Rng;
use std::{io, time::Duration};
use tokio::io::{AsyncWrite, AsyncWriteExt};

pub(super) struct Padding {
    lengths: Vec<usize>,
    gaps: Vec<Duration>,
}
impl Padding {
    pub fn new(ranges: &[PaddingRange]) -> Self {
        let default = [
            PaddingRange {
                probability: 100,
                min: 111,
                max: 1111,
            },
            PaddingRange {
                probability: 75,
                min: 0,
                max: 111,
            },
            PaddingRange {
                probability: 50,
                min: 0,
                max: 3333,
            },
        ];
        let ranges = if ranges.is_empty() {
            &default[..]
        } else {
            ranges
        };
        let mut lengths = Vec::new();
        let mut gaps = Vec::new();
        let mut rng = rand::rng();
        for (index, range) in ranges.iter().enumerate() {
            let amount = if range.probability >= rng.random_range(0..=100) {
                rng.random_range(range.min.min(range.max)..=range.min.max(range.max))
            } else {
                0
            };
            if index % 2 == 0 {
                lengths.push(amount as usize);
            } else {
                gaps.push(Duration::from_millis(u64::from(amount)));
            }
        }
        Self { lengths, gaps }
    }
    pub fn encode(&self, aead: &mut AeadState) -> io::Result<Vec<u8>> {
        let length: usize = self.lengths.iter().sum();
        if !(35..=65553).contains(&length) {
            return Err(invalid("invalid padding length"));
        }
        let mut wire = aead.seal(&((length - 18) as u16).to_be_bytes(), &[])?;
        wire.extend(aead.seal(&vec![0; length - 34], &[])?);
        Ok(wire)
    }
    pub async fn send<S: AsyncWrite + Unpin>(
        self,
        stream: &mut S,
        prefix: Vec<u8>,
        padding: Vec<u8>,
    ) -> io::Result<()> {
        let prefix_len = prefix.len();
        let mut data = prefix;
        data.extend(padding);
        let mut offset = 0;
        for (index, length) in self.lengths.into_iter().enumerate() {
            let length = length + if index == 0 { prefix_len } else { 0 };
            stream.write_all(&data[offset..offset + length]).await?;
            stream.flush().await?;
            offset += length;
            if let Some(gap) = self.gaps.get(index) {
                if !gap.is_zero() {
                    tokio::time::sleep(*gap).await;
                }
            }
        }
        Ok(())
    }
}

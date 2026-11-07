//! Caller-reserved UDP workspaces. Security state changes follow full admission.
use super::ShadowsocksDatagramCodec;
use crate::shared;
use zero_core::{Address, Error};
use zero_traits::{
    DatagramCodecBufferError as Failure, DatagramCodecOutput as Output, PacketBuffer,
    PacketDeriveError,
};
mod address;
mod ordinary;
#[cfg(feature = "blake3")]
mod request;
#[cfg(feature = "blake3")]
mod response;
type Result<T> = core::result::Result<T, Failure<Error>>;
fn protocol(error: Error) -> Failure<Error> {
    Failure::Protocol(error)
}
fn overflow() -> Failure<Error> {
    Failure::Storage(PacketDeriveError::Overflow)
}
fn add(a: usize, b: usize) -> Result<usize> {
    a.checked_add(b).ok_or_else(overflow)
}
fn check(buffers: &[PacketBuffer], lengths: &[usize]) -> Result<()> {
    if buffers.len() != lengths.len() || buffers.iter().zip(lengths).any(|(b, n)| b.len() < *n) {
        return Err(Failure::Storage(PacketDeriveError::LayoutMismatch));
    }
    Ok(())
}
impl ShadowsocksDatagramCodec {
    pub(super) fn encoded_lengths(&self, target: &Address, payload: usize) -> Result<Vec<usize>> {
        let target = add(address::encoded_length(target).map_err(protocol)?, 2)?;
        let data = add(target, payload)?;
        #[cfg(feature = "blake3")]
        if self.cipher.is_blake3() {
            let keys =
                shared::parse_2022_key_chain(self.cipher, &self.password).map_err(protocol)?;
            let padding = if payload == 0 { 899 } else { 0 };
            let header = if self.cipher.is_2022_chacha() { 27 } else { 11 };
            let plain = add(add(header, padding)?, data)?;
            let prefix = if self.cipher.is_2022_chacha() {
                24
            } else {
                add(
                    16,
                    keys.identity_keys
                        .len()
                        .checked_mul(16)
                        .ok_or_else(overflow)?,
                )?
            };
            return Ok(vec![
                plain,
                add(add(prefix, plain)?, self.cipher.tag_len())?,
            ]);
        }
        let tag = if self.cipher.is_stream() {
            0
        } else {
            self.cipher.tag_len()
        };
        Ok(vec![
            data,
            add(add(self.cipher.udp_salt_len(), data)?, tag)?,
        ])
    }
    pub(super) fn decoded_lengths(&self, wire: usize) -> Result<Vec<usize>> {
        // A returned Domain owns its admitted allocation. Its original storage
        // owner remains among the caller's retained output workspaces.
        Ok(vec![wire, 255])
    }
    pub(super) fn encode_reserved(
        &self,
        target: &Address,
        port: u16,
        payload: &[u8],
        buffers: &mut [PacketBuffer],
    ) -> Result<Output> {
        check(buffers, &self.encoded_lengths(target, payload.len())?)?;
        let (plain, wire) = buffers.split_at_mut(1);
        #[cfg(feature = "blake3")]
        if self.cipher.is_blake3() {
            let mut state = self
                .state
                .lock()
                .map_err(|_| protocol(Error::Protocol("ss: UDP session poisoned")))?;
            if let Some(received) = &mut state.received {
                received.prune();
            }
            state
                .sender
                .use_budget(&self.replay.udp_replay_budget())
                .map_err(protocol)?;
            let session = state.sender.next().map_err(protocol)?;
            let length = request::encode(
                self,
                target,
                port,
                payload,
                session,
                &mut plain[0],
                &mut wire[0],
            )
            .map_err(protocol)?;
            return Ok(Output {
                buffer: 1,
                range: 0..length,
            });
        }
        let length = ordinary::encode(self, target, port, payload, &mut plain[0], &mut wire[0])
            .map_err(protocol)?;
        Ok(Output {
            buffer: 1,
            range: 0..length,
        })
    }
    pub(super) fn decode_reserved(
        &self,
        wire: &[u8],
        buffers: &mut [PacketBuffer],
    ) -> Result<Option<(Address, u16, Output)>> {
        check(buffers, &self.decoded_lengths(wire.len())?)?;
        let (plain, domain) = buffers.split_at_mut(1);
        #[cfg(feature = "blake3")]
        if self.cipher.is_blake3() {
            let decoded = match response::decode(self, wire, &mut plain[0], &mut domain[0]) {
                Ok(decoded) => decoded,
                Err(Failure::Protocol(_)) => return Ok(None),
                Err(error) => return Err(error),
            };
            let mut state = self
                .state
                .lock()
                .map_err(|_| protocol(Error::Protocol("ss: UDP session poisoned")))?;
            if !decoded.client.is_some_and(|id| state.sender.matches(id)) {
                return Ok(None);
            }
            if !state
                .received
                .get_or_insert_with(|| {
                    super::ReceiveWindows::with_budget(self.limits, self.replay.udp_replay_budget())
                })
                .accept_with_limits(decoded.session, decoded.packet, self.limits)
                .map_err(protocol)?
            {
                return Ok(None);
            }
            return Ok(Some((
                decoded.target,
                decoded.port,
                Output {
                    buffer: 0,
                    range: decoded.payload,
                },
            )));
        }
        let (length, start) = match ordinary::decrypt(self, wire, &mut plain[0]) {
            Ok(v) => v,
            Err(_) => return Ok(None),
        };
        let (target, port, payload) =
            match address::decode(&plain[0][start..length], &mut domain[0]) {
                Ok(v) => v,
                Err(Failure::Protocol(_)) => return Ok(None),
                Err(error) => return Err(error),
            };
        if self.replay.check(&wire[..self.cipher.salt_len()]).is_err() {
            return Ok(None);
        }
        Ok(Some((
            target,
            port,
            Output {
                buffer: 0,
                range: start + payload..length,
            },
        )))
    }
}

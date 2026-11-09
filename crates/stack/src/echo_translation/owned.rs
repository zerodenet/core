//! Host packet custody pays cache images, fragments and wire transforms before
//! inserting correlation state or returning an output to a bounded queue.
use super::*;
mod batch;
use crate::packet_output::memory::PacketWireMemory;
use batch::derive;
use zero_traits::{PacketDeriveError, PacketStorage};

#[derive(Debug)]
pub struct OwnedTranslationFailure {
    pub source: io::Error,
    pub original: PacketBuffer,
}
pub enum OwnedEchoResponse<C> {
    Unmatched(PacketBuffer),
    Response(C, PacketBuffer),
}
pub(super) struct ResponseMemory {
    maximum: usize,
    mtu: usize,
    wire: PacketWireMemory,
}
impl std::fmt::Display for OwnedTranslationFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.source.fmt(f)
    }
}
impl std::error::Error for OwnedTranslationFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
impl<C> EchoTranslation<C> {
    pub fn response_context(&self, packet: &[u8], now: Instant) -> Option<&C> {
        let key = packet::echo_response_key(packet)?;
        let pending = self.pending.get(&key)?;
        (now < pending.expires).then_some(&pending.context)
    }

    /// Keep the actual incoming allocation and cache until validation and all
    /// outgoing transform storage succeed. A capacity refusal changes neither
    /// the pending correlation entry nor the original packet bytes.
    fn response_storage(
        &mut self,
        original: PacketBuffer,
        mtu: usize,
        wire: PacketWireMemory,
        now: Instant,
        batch: bool,
    ) -> Result<OwnedEchoResponse<C>, OwnedTranslationFailure> {
        let Some(key) = packet::echo_response_key(&original) else {
            return Ok(OwnedEchoResponse::Unmatched(original));
        };
        let Some(pending) = self
            .pending
            .get(&key)
            .filter(|pending| now < pending.expires)
        else {
            return Ok(OwnedEchoResponse::Unmatched(original));
        };
        let Some(response) = &pending.response else {
            return Err(OwnedTranslationFailure {
                source: memory(PacketDeriveError::Unreserved),
                original,
            });
        };
        if original.len() > response.maximum || response.mtu != mtu || response.wire != wire {
            return Err(OwnedTranslationFailure {
                source: memory(PacketDeriveError::LayoutMismatch),
                original,
            });
        }
        let prepared = (|| -> io::Result<Option<PacketBuffer>> {
            // Before the original request entered any FIFO it paid this
            // response's entire transform responsibility on the same envelope.
            let mut outputs =
                derive(&pending.original, &[original.len()], batch).map_err(memory)?;
            if outputs.len() != 1 {
                return Err(memory(PacketDeriveError::LayoutMismatch));
            }
            let mut output = outputs.pop().expect("one exact restored response");
            if packet::restore_echo_response_into(
                &original,
                &pending.original,
                &pending.translated,
                &mut output,
            )
            .is_none()
            {
                return Ok(None);
            }
            Ok(Some(output))
        })();
        let output = match prepared {
            Ok(Some(output)) => output,
            Ok(None) => return Ok(OwnedEchoResponse::Unmatched(original)),
            Err(source) => return Err(OwnedTranslationFailure { source, original }),
        };
        let pending = self
            .pending
            .remove(&key)
            .expect("same validated translation");
        self.bytes -= pending.original.len() + pending.translated.len();
        Ok(OwnedEchoResponse::Response(
            pending.context,
            PacketBuffer::from_owner(Retained {
                packet: output,
                original: pending.original,
                incoming: Some(Arc::new(original)),
            }),
        ))
    }

    fn request_storage(
        &mut self,
        mut original: PacketBuffer,
        local: IpAddr,
        context: C,
        mtu: usize,
        wire: PacketWireMemory,
        return_mtu: usize,
        return_wire: PacketWireMemory,
        now: Instant,
        batch: bool,
    ) -> Result<zero_traits::PacketBatch, OwnedTranslationFailure> {
        self.expire_due(now);
        let prepared = (|| -> io::Result<_> {
            let request = packet::parse_icmp_echo_request(&original)
                .ok_or_else(|| invalid("translation supports ICMP Echo requests only"))?;
            if self.pending.len() >= MAX_REQUESTS
                || self.bytes.saturating_add(original.len().saturating_mul(2)) > MAX_BYTES
            {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "ICMP translation limit",
                ));
            }
            if original.len() > mtu && !packet::ipv4_fragmentation_allowed(&original) {
                return Err(invalid("translated Echo does not permit fragmentation"));
            }
            let key = loop {
                self.next_id = self.next_id.wrapping_add(1);
                let key = (local, request.destination, self.next_id);
                if !self.pending.contains_key(&key) {
                    break key;
                }
            };
            let fragments = packet::fragment_packet_lengths(&original, mtu).map_err(memory)?;
            let mut lengths = vec![original.len(), original.len()];
            lengths.extend_from_slice(&fragments);
            for length in if fragments.is_empty() {
                vec![original.len()]
            } else {
                fragments
            } {
                lengths.extend(wire.workspace_lengths(length).map_err(memory)?);
            }
            let maximum = original.len().max(return_mtu);
            lengths.push(maximum);
            lengths.extend(
                return_wire
                    .return_workspace_lengths(maximum, return_mtu, original[0] >> 4 == 4)
                    .map_err(memory)?,
            );
            let bytes = lengths
                .iter()
                .try_fold(0usize, |sum, n| sum.checked_add(*n))
                .ok_or_else(|| memory(PacketDeriveError::Overflow))?;
            original
                .prepare_transmit(lengths.len(), bytes)
                .map_err(memory)?;
            let mut buffers = derive(&original, &[original.len(), original.len()], batch)
                .map_err(memory)?
                .into_iter();
            let mut output = buffers
                .next()
                .ok_or_else(|| memory(PacketDeriveError::LayoutMismatch))?;
            let mut cached = buffers
                .next()
                .ok_or_else(|| memory(PacketDeriveError::LayoutMismatch))?;
            if buffers.next().is_some() {
                return Err(memory(PacketDeriveError::LayoutMismatch));
            }
            packet::translate_echo_request_into(&original, local, key.2, &mut output)
                .ok_or_else(|| invalid("invalid ICMP address translation"))?;
            cached.copy_from_slice(&output);
            let identification = if output.first().is_some_and(|byte| byte >> 4 == 4) {
                u32::from(u16::from_be_bytes([output[4], output[5]]))
            } else {
                0
            };
            let outputs = if batch {
                packet::fragment_ip_packet_batch(output, mtu, identification)
            } else {
                packet::fragment_ip_packet_prepaid(output, mtu, identification)
                    .map(zero_traits::PacketBatch::from_library)
            }
            .map_err(|failure| memory(failure.cause))?;
            Ok((key, cached, outputs))
        })();
        let (key, cached, outputs) = match prepared {
            Ok(prepared) => prepared,
            Err(source) => return Err(OwnedTranslationFailure { source, original }),
        };
        let original = Arc::new(original);
        self.bytes += original.len() + cached.len();
        let expires = now + REQUEST_TIMEOUT;
        self.next_expiration = Some(self.next_expiration.map_or(expires, |old| old.min(expires)));
        self.pending.insert(
            key,
            Pending {
                original: original.clone(),
                translated: cached,
                context,
                expires,
                response: Some(ResponseMemory {
                    maximum: original.len().max(return_mtu),
                    mtu: return_mtu,
                    wire: return_wire,
                }),
            },
        );
        Ok(outputs.map_in_place(|packet| {
            PacketBuffer::from_owner(Retained {
                packet,
                original: original.clone(),
                incoming: None,
            })
        }))
    }
}
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
fn memory(source: PacketDeriveError) -> io::Error {
    io::Error::new(
        if source == PacketDeriveError::Busy {
            io::ErrorKind::WouldBlock
        } else {
            io::ErrorKind::Other
        },
        source,
    )
}
struct Retained {
    packet: PacketBuffer,
    original: Arc<PacketBuffer>,
    incoming: Option<Arc<PacketBuffer>>,
}
impl AsRef<[u8]> for Retained {
    fn as_ref(&self) -> &[u8] {
        &self.packet
    }
}
impl AsMut<[u8]> for Retained {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.packet
    }
}
impl PacketStorage for Retained {
    fn allocation_bytes(&self) -> usize {
        self.packet
            .allocation_bytes()
            .saturating_add(self.original.allocation_bytes())
            .saturating_add(
                self.incoming
                    .as_ref()
                    .map_or(0, |packet| packet.allocation_bytes()),
            )
    }
    fn prepare_transmit(&mut self, frames: usize, bytes: usize) -> Result<(), PacketDeriveError> {
        self.packet.prepare_transmit(frames, bytes)
    }
    fn try_derive_owned(
        &self,
        lengths: &[usize],
    ) -> Result<zero_traits::PacketBatch, PacketDeriveError> {
        self.packet.try_derive_owned(lengths)
    }
    fn try_derive_zeroed(&self, lengths: &[usize]) -> Result<Vec<PacketBuffer>, PacketDeriveError> {
        self.packet.try_derive_zeroed(lengths)
    }
    fn derive_zeroed<'a>(&'a self, lengths: &'a [usize]) -> zero_traits::PacketDerivation<'a> {
        self.packet.derive_zeroed(lengths)
    }
    fn take_vec_allocation(&mut self) -> Option<Vec<u8>> {
        self.packet.take_vec_allocation()
    }
}
#[cfg(test)]
mod tests;

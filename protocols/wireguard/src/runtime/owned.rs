//! Owned production packet submission. Its storage was admitted as one concrete
//! transform chain before entering the original driver's bounded queue.
mod operation;
use super::*;
use gotatun::packet::WgData;
pub use operation::OwnedTunnelResult;
use zero_traits::PacketDeriveError;

#[derive(Debug)]
pub struct OwnedTunnelFailure {
    pub error: TunnelError,
    pub storage: Option<PacketDeriveError>,
    pub original: Option<PacketBuffer>,
}
impl core::fmt::Display for OwnedTunnelFailure {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "owned WireGuard packet: {:?}", self.error)?;
        if let Some(storage) = self.storage {
            write!(f, ": {storage}")?;
        }
        Ok(())
    }
}
impl std::error::Error for OwnedTunnelFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.storage.as_ref().map(|source| source as _)
    }
}
impl PeerTunnel {
    /// Pure actual codec lengths. The host admits these original allocations
    /// before enqueue; this computes no grant and performs no IO/allocation.
    pub fn transmit_buffer_lengths(&self, length: usize) -> Result<[usize; 2], TunnelError> {
        if length == 0 {
            return Err(TunnelError::EmptyPacket);
        }
        if length > self.mtu {
            return Err(TunnelError::PacketTooLarge);
        }
        let padded = length.div_ceil(16) * 16;
        Ok([padded, WgData::OVERHEAD + padded])
    }
    pub fn send_ip_packet_owned(
        &mut self,
        packet: PacketBuffer,
        control: PacketBuffer,
    ) -> OwnedTunnelResult<Vec<TunnelAction>> {
        if let Err((source, control)) = self.install_control(control) {
            return OwnedTunnelResult {
                result: Err(OwnedTunnelFailure {
                    error: TunnelError::Engine,
                    storage: Some(source),
                    original: Some(packet),
                }),
                unused_control: Some(control),
            };
        }
        let result = self.send_prepared_ip_packet(packet);
        self.finish_owned_operation(result)
    }
    fn send_prepared_ip_packet(
        &mut self,
        packet: PacketBuffer,
    ) -> Result<Vec<TunnelAction>, OwnedTunnelFailure> {
        let lengths =
            self.transmit_buffer_lengths(packet.len())
                .map_err(|error| OwnedTunnelFailure {
                    error,
                    storage: None,
                    original: None,
                });
        let lengths = match lengths {
            Ok(lengths) => lengths,
            Err(mut source) => {
                source.original = Some(packet);
                return Err(source);
            }
        };
        if self.emitted_data_messages >= MAX_DATA_MESSAGES {
            return Err(OwnedTunnelFailure {
                error: TunnelError::MessageLimit,
                storage: None,
                original: Some(packet),
            });
        }
        let mut buffers = match packet.try_derive_zeroed(&lengths) {
            Ok(buffers)
                if buffers.len() == 2
                    && buffers
                        .iter()
                        .zip(lengths)
                        .all(|(buffer, length)| buffer.len() == length) =>
            {
                buffers
            }
            Ok(_) => {
                return Err(OwnedTunnelFailure {
                    error: TunnelError::Engine,
                    storage: Some(PacketDeriveError::LayoutMismatch),
                    original: Some(packet),
                })
            }
            Err(source) => {
                return Err(OwnedTunnelFailure {
                    error: TunnelError::Engine,
                    storage: Some(source),
                    original: Some(packet),
                })
            }
        };
        let cipher = buffers.pop().expect("same admitted ciphertext");
        let mut padded = buffers.pop().expect("same admitted plaintext");
        padded[..packet.len()].copy_from_slice(&packet);
        padded[packet.len()..].fill(0);
        let cipher = match Packet::from_owned_buffer(cipher) {
            Ok(cipher) => cipher,
            Err(_) => {
                return Err(OwnedTunnelFailure {
                    error: TunnelError::Engine,
                    storage: Some(PacketDeriveError::Unreserved),
                    original: Some(packet),
                })
            }
        };
        let mut padded = match Packet::from_owned_buffer(padded) {
            Ok(padded) => padded,
            Err(_) => {
                return Err(OwnedTunnelFailure {
                    error: TunnelError::Engine,
                    storage: Some(PacketDeriveError::Unreserved),
                    original: Some(packet),
                })
            }
        };
        // Both actual codec buffers are already allocated. The SDK consumes
        // this exact cipher buffer and keeps the prior packet in its output.
        padded
            .install_prepaid_cipher(cipher)
            .unwrap_or_else(|_| unreachable!("one actual admitted cipher"));
        padded
            .retain_source(packet)
            .unwrap_or_else(|_| unreachable!("one actual source allocation"));
        let result = self.engine.handle_outgoing_packet(padded, None);
        result
            .map(|packet| self.collect_result(TunnResult::WriteToNetwork(packet)))
            .unwrap_or_else(|| Ok(Vec::new()))
            .map_err(|error| OwnedTunnelFailure {
                error,
                storage: None,
                original: None,
            })
    }
}

pub(super) fn receive_scratch(
    original: &Arc<PacketBuffer>,
    mut decode: zero_tun::packet_memory::NativeReadBuffer,
    data: bool,
) -> Result<Packet, PacketDeriveError> {
    if decode.as_mut_bytes().len() < original.len() {
        return Err(PacketDeriveError::LayoutMismatch);
    }
    decode.as_mut_bytes()[..original.len()].copy_from_slice(original);
    let mut buffer = decode
        .into_packet(original.len())
        .map_err(|_| PacketDeriveError::LayoutMismatch)?;
    if data {
        buffer.prepare_transmit(0, 0)?;
    }
    let mut packet =
        Packet::from_owned_buffer(buffer).map_err(|_| PacketDeriveError::Unreserved)?;
    packet
        .retain_shared_source(original.clone())
        .unwrap_or_else(|_| unreachable!("one original ciphertext"));
    Ok(packet)
}

// Kept protocol-private: neither the Host nor generic memory storage knows
// WireGuard packet types. Full structural/MAC/replay validation stays in SDK.
pub(super) fn is_data_datagram(packet: &[u8]) -> bool {
    packet.len() >= WgData::OVERHEAD && packet.starts_with(&4_u32.to_le_bytes())
}

#[cfg(test)]
mod tests;

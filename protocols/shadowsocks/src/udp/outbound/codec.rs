use super::*;
#[cfg(feature = "crypto")]
impl<'a> UdpDatagramFraming<ShadowsocksUdpPacketTarget<'a>, ShadowsocksUdpDecodeContext<'a>>
    for ShadowsocksOutbound
{
    type Error = Error;
    type Decoded = ShadowsocksUdpPacket;

    fn encode_udp_datagram(
        &self,
        packet: &ShadowsocksUdpPacketTarget<'a>,
    ) -> Result<Vec<u8>, Self::Error> {
        use crate::shared::{aead_encrypt_udp, build_target_data, derive_udp_packet_key};

        if packet.cipher.is_blake3() {
            return crate::shared::encode_udp_datagram_2022(
                packet.cipher,
                packet.password,
                packet.target,
                packet.port,
                packet.payload,
            );
        }

        let target_data = build_target_data(packet.target, packet.port, packet.payload)?;
        if packet.cipher.is_stream() {
            return crate::shared::legacy::encode_datagram(
                packet.cipher,
                packet.password,
                &target_data,
            );
        }
        let mut salt = vec![0u8; packet.cipher.udp_salt_len()];
        use ring::rand::SecureRandom;
        ring::rand::SystemRandom::new()
            .fill(&mut salt)
            .map_err(|_| Error::Protocol("ss: random failed"))?;

        let key = derive_udp_packet_key(packet.cipher, packet.password, &salt)?;

        let encrypted = aead_encrypt_udp(packet.cipher, &key, &[0u8; 12], &target_data)?;
        let mut datagram = Vec::with_capacity(salt.len() + encrypted.len());
        datagram.extend_from_slice(&salt);
        datagram.extend_from_slice(&encrypted);
        Ok(datagram)
    }

    fn decode_udp_datagram(
        &self,
        context: &ShadowsocksUdpDecodeContext<'a>,
        datagram: &[u8],
    ) -> Result<Self::Decoded, Self::Error> {
        use crate::shared::{aead_decrypt_udp, derive_udp_packet_key, parse_target_data};

        if context.cipher.is_blake3() {
            let (target, port, payload) = crate::shared::decode_udp_datagram_2022(
                context.cipher,
                context.password,
                datagram,
            )?;
            return Ok(ShadowsocksUdpPacket::new(target, port, payload));
        }

        if context.cipher.is_stream() {
            let plain =
                crate::shared::legacy::decode_datagram(context.cipher, context.password, datagram)?;
            let (target, port, offset) = parse_target_data(&plain)?;
            return Ok(ShadowsocksUdpPacket::new(
                target,
                port,
                plain[offset..].to_vec(),
            ));
        }
        let salt_len = context.cipher.udp_salt_len();
        if datagram.len() < salt_len + context.cipher.tag_len() {
            return Err(Error::Protocol("ss: udp datagram too short"));
        }

        let key = derive_udp_packet_key(context.cipher, context.password, &datagram[..salt_len])?;

        let plain = aead_decrypt_udp(context.cipher, &key, &[0u8; 12], &datagram[salt_len..])?;
        let (target, port, payload_offset) = parse_target_data(&plain)?;
        Ok(ShadowsocksUdpPacket::new(
            target,
            port,
            plain[payload_offset..].to_vec(),
        ))
    }
}

#[cfg(feature = "crypto")]
pub(super) fn udp_cache_key(
    tag: &str,
    server: &str,
    port: u16,
    cipher: &str,
    password: &str,
) -> String {
    alloc::format!(
        "shadowsocks:{}",
        crate::shared::cache_identity([
            tag.as_bytes(),
            server.as_bytes(),
            &port.to_be_bytes(),
            cipher.as_bytes(),
            password.as_bytes()
        ])
    )
}

#[cfg(feature = "crypto")]
pub fn parse_udp_cipher(cipher: &str) -> Result<crate::shared::CipherKind, Error> {
    crate::shared::CipherKind::from_str(cipher).ok_or(Error::Protocol("ss: unknown udp cipher"))
}

/// Codec state for a Shadowsocks UDP datagram chain hop.
///
/// Captures the cipher and password needed to encode/decode Shadowsocks
/// UDP datagrams in a relay chain. Implements [`DatagramCodec`] from
/// `zero-traits` so the proxy runtime can use it without protocol-specific
/// adapter code.
pub use crate::udp::client::ShadowsocksDatagramCodec;

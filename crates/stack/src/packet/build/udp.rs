use super::*;
#[derive(Clone, Copy)]
pub struct UdpPacketSpec<'a> {
    pub source: IpAddr,
    pub destination: IpAddr,
    pub source_port: u16,
    pub destination_port: u16,
    pub payload: &'a [u8],
}
impl UdpPacketSpec<'_> {
    pub fn length(&self) -> Result<usize, PacketDeriveError> {
        let ip = ip_header(self.source, self.destination)?;
        let length = ip
            .checked_add(8)
            .and_then(|length| length.checked_add(self.payload.len()))
            .ok_or(PacketDeriveError::Overflow)?;
        let field = if ip == 20 { length } else { length - ip };
        if field > usize::from(u16::MAX) {
            return Err(PacketDeriveError::Capacity);
        }
        Ok(length)
    }
    pub fn write_into(&self, output: &mut [u8]) -> Result<(), PacketDeriveError> {
        if output.len() != self.length()? {
            return Err(PacketDeriveError::LayoutMismatch);
        }
        output.fill(0);
        fill_ip(output, self.source, self.destination, IPPROTO_UDP);
        let offset = ip_header(self.source, self.destination)?;
        let length = (output.len() - offset) as u16;
        output[offset..offset + 2].copy_from_slice(&self.source_port.to_be_bytes());
        output[offset + 2..offset + 4].copy_from_slice(&self.destination_port.to_be_bytes());
        output[offset + 4..offset + 6].copy_from_slice(&length.to_be_bytes());
        output[offset + 8..].copy_from_slice(self.payload);
        let sum = match (self.source, self.destination) {
            (IpAddr::V4(source), IpAddr::V4(destination)) => {
                udp_checksum_v4(&source, &destination, &output[offset..])
            }
            (IpAddr::V6(source), IpAddr::V6(destination)) => {
                udp_checksum_v6(&source, &destination, &output[offset..])
            }
            _ => unreachable!("same validated IP family"),
        };
        output[offset + 6..offset + 8].copy_from_slice(&sum.to_be_bytes());
        Ok(())
    }
}

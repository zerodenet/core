use super::*;
#[derive(Clone, Copy)]
pub struct TcpPacketSpec<'a> {
    pub source: IpAddr,
    pub destination: IpAddr,
    pub source_port: u16,
    pub destination_port: u16,
    pub sequence: u32,
    pub acknowledgment: u32,
    pub flags: u8,
    pub window: u16,
    pub mss: Option<u16>,
    pub payload: &'a [u8],
}
impl TcpPacketSpec<'_> {
    pub fn length(&self) -> Result<usize, PacketDeriveError> {
        let ip = ip_header(self.source, self.destination)?;
        let tcp = 20usize + if self.mss.is_some() { 4 } else { 0 };
        let length = ip
            .checked_add(tcp)
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
        fill_ip(output, self.source, self.destination, IPPROTO_TCP);
        let offset = ip_header(self.source, self.destination)?;
        let header = if self.mss.is_some() { 24 } else { 20 };
        output[offset..offset + 2].copy_from_slice(&self.source_port.to_be_bytes());
        output[offset + 2..offset + 4].copy_from_slice(&self.destination_port.to_be_bytes());
        output[offset + 4..offset + 8].copy_from_slice(&self.sequence.to_be_bytes());
        output[offset + 8..offset + 12].copy_from_slice(&self.acknowledgment.to_be_bytes());
        output[offset + 12] = (header / 4) << 4;
        output[offset + 13] = self.flags;
        output[offset + 14..offset + 16].copy_from_slice(&self.window.to_be_bytes());
        if let Some(mss) = self.mss {
            output[offset + 20] = 2;
            output[offset + 21] = 4;
            output[offset + 22..offset + 24].copy_from_slice(&mss.to_be_bytes());
        }
        output[offset + usize::from(header)..].copy_from_slice(self.payload);
        let sum = match (self.source, self.destination) {
            (IpAddr::V4(source), IpAddr::V4(destination)) => {
                tcp_checksum_v4(&source, &destination, &output[offset..])
            }
            (IpAddr::V6(source), IpAddr::V6(destination)) => {
                tcp_checksum_v6(&source, &destination, &output[offset..])
            }
            _ => unreachable!("same validated IP family"),
        };
        output[offset + 16..offset + 18].copy_from_slice(&sum.to_be_bytes());
        Ok(())
    }
}

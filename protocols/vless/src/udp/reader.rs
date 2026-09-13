//! Keep inbound frame progress when the runtime selects an upstream response.
use super::*;
use tokio::io::{AsyncRead, AsyncReadExt};

#[derive(Default)]
pub(super) struct PacketReader {
    length: [u8; 2],
    header_read: usize,
    payload: Vec<u8>,
    payload_read: usize,
}

impl PacketReader {
    pub(super) async fn read<R: AsyncRead + Unpin>(
        &mut self,
        reader: &mut R,
        target: &Address,
        port: u16,
    ) -> Result<Option<VlessUdpFlowPacket>, Error> {
        while self.header_read < 2 {
            let read = reader
                .read(&mut self.length[self.header_read..])
                .await
                .map_err(|_| Error::Io("vless udp read length"))?;
            if read == 0 {
                return if self.header_read == 0 {
                    Ok(None)
                } else {
                    Err(Error::Io("truncated vless udp length"))
                };
            }
            self.header_read += read;
        }
        let length = u16::from_be_bytes(self.length) as usize;
        self.payload.resize(length, 0);
        while self.payload_read < length {
            let read = reader
                .read(&mut self.payload[self.payload_read..])
                .await
                .map_err(|_| Error::Io("vless udp read payload"))?;
            if read == 0 {
                return Err(Error::Io("truncated vless udp payload"));
            }
            self.payload_read += read;
        }
        self.header_read = 0;
        self.payload_read = 0;
        Ok(Some(VlessUdpFlowPacket::new(
            target.clone(),
            port,
            core::mem::take(&mut self.payload),
        )))
    }
}

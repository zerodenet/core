//! Cancellation-safe framing over a logical byte stream, independent of carrier fragments.
use tokio::io::{AsyncRead, AsyncReadExt};
use zero_core::Error;

pub(super) async fn read_packet<R: AsyncRead + Unpin>(
    reader: &mut R,
    pending: &mut Vec<u8>,
) -> Result<Option<Vec<u8>>, Error> {
    loop {
        let needed = if pending.len() < 3 {
            3
        } else {
            if pending[0] != 0 {
                return Err(Error::Protocol("mieru udp: missing start marker"));
            }
            4 + u16::from_be_bytes([pending[1], pending[2]]) as usize
        };
        if pending.len() == needed && needed >= 4 {
            return Ok(Some(std::mem::take(pending)));
        }
        let mut chunk = [0; 4096];
        let limit = chunk.len().min(needed - pending.len());
        let n = reader
            .read(&mut chunk[..limit])
            .await
            .map_err(|_| Error::Io("failed to read Mieru UDP request"))?;
        if n == 0 {
            return if pending.is_empty() {
                Ok(None)
            } else {
                Err(Error::Protocol("mieru udp: truncated"))
            };
        }
        pending.extend_from_slice(&chunk[..n]);
    }
}

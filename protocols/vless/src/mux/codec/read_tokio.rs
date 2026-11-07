use super::{parse_metadata, Error, MuxFrame, MUX_MAX_METADATA, MUX_MAX_PAYLOAD, OPTION_DATA};
use crate::mux::backlog::{BufferedMuxResponse, MuxResponseBacklog};
use tokio::io::{AsyncRead, AsyncReadExt};

pub(crate) enum BufferedFrame {
    Frame(BufferedMuxResponse<MuxFrame>),
    Overflow(u16),
}

async fn header<R: AsyncRead + Unpin>(reader: &mut R) -> Result<(MuxFrame, usize), Error> {
    let mut metadata_len = [0_u8; 2];
    reader
        .read_exact(&mut metadata_len)
        .await
        .map_err(|_| Error::Io("failed to read MUX metadata length"))?;
    let metadata_len = u16::from_be_bytes(metadata_len) as usize;
    if !(4..=MUX_MAX_METADATA).contains(&metadata_len) {
        return Err(Error::Protocol("MUX metadata length is invalid"));
    }
    let mut metadata = alloc::vec![0_u8; metadata_len];
    reader
        .read_exact(&mut metadata)
        .await
        .map_err(|_| Error::Io("failed to read MUX metadata"))?;
    let frame = parse_metadata(&metadata, false)?;
    let data_len = if frame.options & OPTION_DATA != 0 {
        let mut data_len = [0_u8; 2];
        reader
            .read_exact(&mut data_len)
            .await
            .map_err(|_| Error::Io("failed to read MUX data length"))?;
        let data_len = u16::from_be_bytes(data_len) as usize;
        if data_len > MUX_MAX_PAYLOAD {
            return Err(Error::Protocol("MUX frame payload is too large"));
        }
        data_len
    } else {
        0
    };
    Ok((frame, data_len))
}

pub(crate) async fn read_frame_tokio<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<MuxFrame, Error> {
    let (mut frame, length) = header(reader).await?;
    frame.payload.resize(length, 0);
    reader
        .read_exact(&mut frame.payload)
        .await
        .map_err(|_| Error::Io("failed to read MUX frame payload"))?;
    Ok(frame)
}

pub(crate) async fn read_buffered_frame<R: AsyncRead + Unpin, T>(
    reader: &mut R,
    backlog: &MuxResponseBacklog,
    capture: impl FnOnce(&MuxFrame) -> T,
) -> Result<(BufferedFrame, T), Error> {
    let (mut frame, length) = header(reader).await?;
    // Pin the actual recipient before either byte/FIFO wait can allow SID reuse.
    let recipient = capture(&frame);
    let mut waiter = None;
    let credit = core::future::poll_fn(|cx| backlog.poll_charge(cx, length, &mut waiter)).await;
    let Some(credit) = credit else {
        // A frame larger than the declared byte cap cannot become admissible.
        // Consume only this rejected body, retaining wire framing for other SIDs.
        let mut scratch = [0u8; 1024];
        let mut remaining = length;
        while remaining != 0 {
            let count = remaining.min(scratch.len());
            reader
                .read_exact(&mut scratch[..count])
                .await
                .map_err(|_| Error::Io("failed to read MUX frame payload"))?;
            remaining -= count;
        }
        return Ok((BufferedFrame::Overflow(frame.session_id), recipient));
    };
    frame.payload.resize(length, 0);
    reader
        .read_exact(&mut frame.payload)
        .await
        .map_err(|_| Error::Io("failed to read MUX frame payload"))?;
    Ok((BufferedFrame::Frame(credit.map(|()| frame)), recipient))
}

use super::*;

pub(super) enum BufferedFrame {
    Frame(BufferedMuxResponse<MuxFrame>),
    Overflow(u16),
}

async fn header<R: AsyncRead + Unpin>(reader: &mut R) -> Result<(MuxFrame, usize), Error> {
    let mut len_buf = [0u8; 2];
    tokio::io::AsyncReadExt::read_exact(reader, &mut len_buf)
        .await
        .map_err(|_| Error::Io("vmess: failed to read from socket"))?;
    let meta_len = u16::from_be_bytes(len_buf) as usize;
    if meta_len > MUX_MAX_META_LEN {
        return Err(Error::Protocol("vmess mux metadata too large"));
    }

    let mut meta = vec![0_u8; meta_len];
    tokio::io::AsyncReadExt::read_exact(reader, &mut meta)
        .await
        .map_err(|_| Error::Io("vmess: failed to read from socket"))?;
    let frame = decode_metadata(&meta)?;

    let length = if frame.option & MUX_OPTION_DATA != 0 {
        tokio::io::AsyncReadExt::read_exact(reader, &mut len_buf)
            .await
            .map_err(|_| Error::Io("vmess: failed to read from socket"))?;
        let data_len = u16::from_be_bytes(len_buf) as usize;
        if data_len > MUX_MAX_DATA_LEN {
            return Err(Error::Protocol("vmess mux data too large"));
        }
        data_len
    } else {
        0
    };
    Ok((frame, length))
}

pub(super) async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R) -> Result<MuxFrame, Error> {
    let (mut frame, length) = header(reader).await?;
    frame.payload.resize(length, 0);
    tokio::io::AsyncReadExt::read_exact(reader, &mut frame.payload)
        .await
        .map_err(|_| Error::Io("vmess: failed to read from socket"))?;
    Ok(frame)
}

pub(super) async fn read_buffered<R: AsyncRead + Unpin, T>(
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
        // Never allocate an inadmissible payload or lose the following SID's framing.
        let mut scratch = [0u8; 1024];
        let mut remaining = length;
        while remaining != 0 {
            let count = remaining.min(scratch.len());
            tokio::io::AsyncReadExt::read_exact(reader, &mut scratch[..count])
                .await
                .map_err(|_| Error::Io("vmess: failed to read from socket"))?;
            remaining -= count;
        }
        return Ok((BufferedFrame::Overflow(frame.session_id), recipient));
    };
    frame.payload.resize(length, 0);
    tokio::io::AsyncReadExt::read_exact(reader, &mut frame.payload)
        .await
        .map_err(|_| Error::Io("vmess: failed to read from socket"))?;
    Ok((BufferedFrame::Frame(credit.map(|()| frame)), recipient))
}

use alloc::vec::Vec;

use zero_core::{Address, Error};
use zero_traits::AsyncSocket;

use super::{
    parse_address_from_bytes_with_len, MuxFrame, MuxTarget, MUX_MAX_METADATA, MUX_MAX_PAYLOAD,
    NETWORK_UDP, OPTION_DATA, STATUS_KEEP, STATUS_NEW,
};
use crate::shared::{read_exact, write_address};

fn checked_u16(value: usize, message: &'static str) -> Result<u16, Error> {
    u16::try_from(value).map_err(|_| Error::Protocol(message))
}

fn parse_metadata(metadata: &[u8]) -> Result<MuxFrame, Error> {
    if metadata.len() < 4 {
        return Err(Error::Protocol("MUX metadata is shorter than 4 bytes"));
    }

    let session_id = u16::from_be_bytes([metadata[0], metadata[1]]);
    let status = metadata[2];
    let options = metadata[3];
    let mut offset = 4;
    let mut target = None;

    let carries_target = status == STATUS_NEW
        || (status == STATUS_KEEP
            && metadata
                .get(offset)
                .is_some_and(|network| *network == NETWORK_UDP));
    if carries_target {
        let network = *metadata
            .get(offset)
            .ok_or(Error::Protocol("MUX target network is missing"))?;
        offset += 1;
        let port_bytes = metadata
            .get(offset..offset + 2)
            .ok_or(Error::Protocol("MUX target port is truncated"))?;
        let port = u16::from_be_bytes([port_bytes[0], port_bytes[1]]);
        offset += 2;
        if port == 0 {
            return Err(Error::Protocol("MUX target port must not be 0"));
        }
        let atyp = *metadata
            .get(offset)
            .ok_or(Error::Protocol("MUX target address type is missing"))?;
        offset += 1;
        let (address, consumed) = parse_address_from_bytes_with_len(atyp, &metadata[offset..])?;
        offset += consumed;
        target = Some(MuxTarget {
            network,
            port,
            address,
        });
    }

    let global_id = if status == STATUS_NEW
        && target
            .as_ref()
            .is_some_and(|target| target.network == NETWORK_UDP)
        && metadata.len().saturating_sub(offset) >= 8
    {
        let mut global_id = [0_u8; 8];
        global_id.copy_from_slice(&metadata[offset..offset + 8]);
        Some(global_id)
    } else {
        None
    };

    Ok(MuxFrame {
        session_id,
        status,
        options,
        target,
        global_id,
        payload: Vec::new(),
    })
}

fn encode_metadata(
    session_id: u16,
    status: u8,
    options: u8,
    target: Option<&MuxTarget>,
    global_id: Option<[u8; 8]>,
) -> Result<Vec<u8>, Error> {
    if status == STATUS_NEW && target.is_none() {
        return Err(Error::Protocol("MUX new stream target is required"));
    }

    let mut metadata = Vec::with_capacity(32);
    metadata.extend_from_slice(&session_id.to_be_bytes());
    metadata.push(status);
    metadata.push(options);
    if let Some(target) = target {
        metadata.push(target.network);
        metadata.extend_from_slice(&target.port.to_be_bytes());
        write_address(&mut metadata, &target.address)?;
    }
    if let Some(global_id) = global_id {
        metadata.extend_from_slice(&global_id);
    }
    if metadata.len() > MUX_MAX_METADATA {
        return Err(Error::Protocol("MUX metadata is too large"));
    }
    Ok(metadata)
}

fn encode_frame(
    session_id: u16,
    status: u8,
    options: u8,
    target: Option<&MuxTarget>,
    global_id: Option<[u8; 8]>,
    payload: &[u8],
) -> Result<Vec<u8>, Error> {
    if payload.len() > MUX_MAX_PAYLOAD {
        return Err(Error::Protocol("MUX frame payload is too large"));
    }
    if options & OPTION_DATA == 0 && !payload.is_empty() {
        return Err(Error::Protocol(
            "MUX frame payload requires the data option",
        ));
    }

    let metadata = encode_metadata(session_id, status, options, target, global_id)?;
    let metadata_len = checked_u16(metadata.len(), "MUX metadata length exceeds u16")?;
    let data_len = checked_u16(payload.len(), "MUX data length exceeds u16")?;
    let mut frame = Vec::with_capacity(
        2 + metadata.len() + if options & OPTION_DATA != 0 { 2 } else { 0 } + payload.len(),
    );
    frame.extend_from_slice(&metadata_len.to_be_bytes());
    frame.extend_from_slice(&metadata);
    if options & OPTION_DATA != 0 {
        frame.extend_from_slice(&data_len.to_be_bytes());
        frame.extend_from_slice(payload);
    }
    Ok(frame)
}

pub(super) fn encode_new_stream(
    session_id: u16,
    network: u8,
    port: u16,
    address: &Address,
) -> Result<Vec<u8>, Error> {
    encode_frame(
        session_id,
        STATUS_NEW,
        0,
        Some(&MuxTarget {
            network,
            port,
            address: address.clone(),
        }),
        None,
        &[],
    )
}

pub(super) fn encode_new_udp_data_frame(
    session_id: u16,
    target: &Address,
    port: u16,
    global_id: [u8; 8],
    payload: &[u8],
) -> Result<Vec<u8>, Error> {
    encode_frame(
        session_id,
        STATUS_NEW,
        OPTION_DATA,
        Some(&MuxTarget {
            network: NETWORK_UDP,
            port,
            address: target.clone(),
        }),
        Some(global_id),
        payload,
    )
}

pub(super) fn encode_keep_udp_data_frame(
    session_id: u16,
    target: &Address,
    port: u16,
    payload: &[u8],
) -> Result<Vec<u8>, Error> {
    encode_frame(
        session_id,
        STATUS_KEEP,
        OPTION_DATA,
        Some(&MuxTarget {
            network: NETWORK_UDP,
            port,
            address: target.clone(),
        }),
        None,
        payload,
    )
}

pub(super) fn encode_data_frame(session_id: u16, payload: &[u8]) -> Result<Vec<u8>, Error> {
    encode_frame(session_id, STATUS_KEEP, OPTION_DATA, None, None, payload)
}

pub(super) fn encode_end_frame(session_id: u16) -> Result<Vec<u8>, Error> {
    encode_frame(session_id, super::STATUS_END, 0, None, None, &[])
}

async fn read_data<S>(stream: &mut S, frame: &mut MuxFrame) -> Result<(), Error>
where
    S: AsyncSocket,
{
    if frame.options & OPTION_DATA == 0 {
        return Ok(());
    }
    let mut data_len = [0_u8; 2];
    read_exact(stream, &mut data_len).await?;
    let data_len = u16::from_be_bytes(data_len) as usize;
    if data_len > MUX_MAX_PAYLOAD {
        return Err(Error::Protocol("MUX frame payload is too large"));
    }
    frame.payload.resize(data_len, 0);
    if data_len > 0 {
        read_exact(stream, &mut frame.payload).await?;
    }
    Ok(())
}

pub(super) async fn read_frame<S>(stream: &mut S) -> Result<MuxFrame, Error>
where
    S: AsyncSocket,
{
    let mut metadata_len = [0_u8; 2];
    read_exact(stream, &mut metadata_len).await?;
    let metadata_len = u16::from_be_bytes(metadata_len) as usize;
    if !(4..=MUX_MAX_METADATA).contains(&metadata_len) {
        return Err(Error::Protocol("MUX metadata length is invalid"));
    }
    let mut metadata = alloc::vec![0_u8; metadata_len];
    read_exact(stream, &mut metadata).await?;
    let mut frame = parse_metadata(&metadata)?;
    read_data(stream, &mut frame).await?;
    Ok(frame)
}

#[cfg(feature = "reality")]
pub(super) async fn read_frame_tokio<R>(reader: &mut R) -> Result<MuxFrame, Error>
where
    R: tokio::io::AsyncRead + Unpin,
{
    use tokio::io::AsyncReadExt;

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
    let mut frame = parse_metadata(&metadata)?;
    if frame.options & OPTION_DATA != 0 {
        let mut data_len = [0_u8; 2];
        reader
            .read_exact(&mut data_len)
            .await
            .map_err(|_| Error::Io("failed to read MUX data length"))?;
        let data_len = u16::from_be_bytes(data_len) as usize;
        if data_len > MUX_MAX_PAYLOAD {
            return Err(Error::Protocol("MUX frame payload is too large"));
        }
        frame.payload.resize(data_len, 0);
        if data_len > 0 {
            reader
                .read_exact(&mut frame.payload)
                .await
                .map_err(|_| Error::Io("failed to read MUX frame payload"))?;
        }
    }
    Ok(frame)
}

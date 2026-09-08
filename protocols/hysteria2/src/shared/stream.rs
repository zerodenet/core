use alloc::vec;

use zero_core::{Address, Error};
use zero_traits::AsyncSocket;

use super::{decode_varint, parse_authority, read_exact, MAX_ADDRESS_LENGTH, MAX_PADDING_LENGTH};

/// Read only TCPRequest bytes, leaving any coalesced application data in the stream.
pub(crate) async fn read_tcp_connect_header<S: AsyncSocket>(
    stream: &mut S,
) -> Result<(Address, u16), Error> {
    if read_varint(stream).await? != 0x401 {
        return Err(Error::Protocol("hysteria2: expected TCPRequest"));
    }
    let address_len = read_bounded_length(
        stream,
        MAX_ADDRESS_LENGTH,
        "hysteria2: invalid address length",
    )
    .await?;
    if address_len == 0 {
        return Err(Error::Protocol("hysteria2: invalid address length"));
    }
    let mut address = vec![0; address_len];
    read_exact(stream, &mut address).await?;
    let authority = core::str::from_utf8(&address)
        .map_err(|_| Error::Protocol("hysteria2: invalid TCPRequest address"))?;
    let target = parse_authority(authority)?;
    let padding_len = read_bounded_length(
        stream,
        MAX_PADDING_LENGTH,
        "hysteria2: invalid padding length",
    )
    .await?;
    discard_exact(stream, padding_len).await?;
    Ok(target)
}

async fn read_varint<S: AsyncSocket>(stream: &mut S) -> Result<u64, Error> {
    let mut encoded = [0; 8];
    read_exact(stream, &mut encoded[..1]).await?;
    let width = 1usize << (encoded[0] >> 6);
    read_exact(stream, &mut encoded[1..width]).await?;
    decode_varint(&encoded[..width]).map(|(value, _)| value)
}

pub(crate) async fn read_bounded_length<S: AsyncSocket>(
    stream: &mut S,
    maximum: usize,
    error: &'static str,
) -> Result<usize, Error> {
    let value = read_varint(stream).await?;
    if value > maximum as u64 {
        return Err(Error::Protocol(error));
    }
    Ok(value as usize)
}

pub(crate) async fn discard_exact<S: AsyncSocket>(
    stream: &mut S,
    mut remaining: usize,
) -> Result<(), Error> {
    let mut buffer = [0; 256];
    while remaining > 0 {
        let take = remaining.min(buffer.len());
        read_exact(stream, &mut buffer[..take]).await?;
        remaining -= take;
    }
    Ok(())
}

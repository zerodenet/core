//! RFC 8879 certificate decoding. Transcripts retain the compressed wire message.
use std::{borrow::Cow, io};
const LIMIT: usize = 256 * 1024;
fn invalid() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "invalid compressed TLS certificate",
    )
}
fn u24(bytes: &[u8]) -> usize {
    u32::from_be_bytes([0, bytes[0], bytes[1], bytes[2]]) as usize
}
/// Reconstruct the ordinary Certificate handshake message for validation only.
/// All advertised algorithms are supported; decompression has a fixed output cap.
pub fn decode(message: &[u8]) -> io::Result<Cow<'_, [u8]>> {
    if message.first() == Some(&11) {
        return Ok(Cow::Borrowed(message));
    }
    if message.len() < 13 || message[0] != 25 || u24(&message[1..4]) + 4 != message.len() {
        return Err(invalid());
    }
    let length = u24(&message[6..9]);
    let compressed = u24(&message[9..12]);
    if !(4..=LIMIT - 4).contains(&length)
        || compressed == 0
        || compressed > LIMIT
        || compressed + 12 != message.len()
    {
        return Err(invalid());
    }
    let mut decoded = vec![0; length + 4];
    decoded[0] = 11;
    decoded[1..4].copy_from_slice(&message[6..9]);
    match u16::from_be_bytes([message[4], message[5]]) {
        1 => {
            // One bounded output allocation, and require the complete zlib stream.
            let mut decoder = flate2::Decompress::new(true);
            let status = decoder
                .decompress(
                    &message[12..],
                    &mut decoded[4..],
                    flate2::FlushDecompress::Finish,
                )
                .map_err(|_| invalid())?;
            if status != flate2::Status::StreamEnd
                || decoder.total_in() as usize != compressed
                || decoder.total_out() as usize != length
            {
                return Err(invalid());
            }
        }
        2 => {
            // Keep client decoding independent of Rustls feature unification:
            // enabling rustls/brotli also changes every default server's flight.
            let mut output = io::Cursor::new(&mut decoded[4..]);
            brotli_decompressor::BrotliDecompress(
                &mut io::Cursor::new(&message[12..]),
                &mut output,
            )
            .map_err(|_| invalid())?;
            if output.position() as usize != length {
                return Err(invalid());
            }
        }
        3 => {
            // Bulk decompression writes directly into this bounded destination;
            // it does not allocate the window declared in the peer's frame.
            let written = zstd::bulk::Decompressor::new()?
                .decompress_to_buffer(&message[12..], &mut decoded[4..])?;
            if written != length {
                return Err(invalid());
            }
        }
        _ => return Err(invalid()),
    }
    super::server_chain(&decoded)?;
    Ok(Cow::Owned(decoded))
}

#[cfg(test)]
#[path = "../../tests/unit/certificate_compression.rs"]
mod tests;

/// Bounded certificate decompressors shared with the ordinary TLS state machine.
pub static RUSTLS_DECOMPRESSORS: &[&dyn rustls::compress::CertDecompressor] = &[
    &RustlsDecompressor(1),
    &RustlsDecompressor(2),
    &RustlsDecompressor(3),
];
#[derive(Debug)]
struct RustlsDecompressor(u16);
impl rustls::compress::CertDecompressor for RustlsDecompressor {
    fn algorithm(&self) -> rustls::CertificateCompressionAlgorithm {
        self.0.into()
    }
    fn decompress(
        &self,
        input: &[u8],
        output: &mut [u8],
    ) -> Result<(), rustls::compress::DecompressionFailed> {
        let failed = || rustls::compress::DecompressionFailed;
        if input.is_empty() || input.len() > LIMIT || output.len() > LIMIT - 4 {
            return Err(failed());
        }
        let mut message = vec![25];
        message.extend_from_slice(&((input.len() + 8) as u32).to_be_bytes()[1..]);
        message.extend_from_slice(&self.0.to_be_bytes());
        message.extend_from_slice(&(output.len() as u32).to_be_bytes()[1..]);
        message.extend_from_slice(&(input.len() as u32).to_be_bytes()[1..]);
        message.extend_from_slice(input);
        let decoded = decode(&message).map_err(|_| failed())?;
        if decoded.len() != output.len() + 4 {
            return Err(failed());
        }
        output.copy_from_slice(&decoded[4..]);
        Ok(())
    }
}

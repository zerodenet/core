//! Pure mask transforms shared by the original hosted and library carriers.
use super::*;
use tokio::io::AsyncWrite;
pub(super) mod library;
pub(super) enum Transform {
    Fragment(Fragment),
    Sudoku(super::super::sudoku::Profile, bool),
}
pub(super) enum Decoder {
    Copy,
    Pure(super::super::sudoku::Decoder),
    Packed(super::super::sudoku::PackedDecoder),
}
impl Decoder {
    pub(super) fn decode(&mut self, bytes: &[u8]) -> io::Result<Vec<u8>> {
        match self {
            Self::Copy => Ok(bytes.to_vec()),
            Self::Pure(decoder) => decoder.decode(bytes),
            Self::Packed(decoder) => Ok(decoder.decode(bytes)),
        }
    }
}
pub(super) enum Encoder {
    Fragment(Fragment, u64),
    Pure(super::super::sudoku::Encoder),
    Packed(super::super::sudoku::PackedEncoder),
}
impl Encoder {
    pub(super) async fn write<W: AsyncWrite + Unpin>(
        &mut self,
        writer: &mut W,
        bytes: &[u8],
    ) -> io::Result<()> {
        match self {
            Self::Fragment(config, count) => {
                *count = count.saturating_add(1);
                fragment::write(writer, config, *count, bytes).await
            }
            Self::Pure(encoder) => writer.write_all(&encoder.encode(bytes)).await,
            Self::Packed(encoder) => writer.write_all(&encoder.encode(bytes)).await,
        }
    }
}

impl Transform {
    pub(super) fn codecs(self) -> (Decoder, Encoder) {
        match self {
            Self::Fragment(config) => (Decoder::Copy, Encoder::Fragment(config, 0)),
            Self::Sudoku(profile, true) => (
                Decoder::Pure(profile.decoder()),
                Encoder::Packed(profile.packed_encoder()),
            ),
            Self::Sudoku(profile, false) => (
                Decoder::Packed(profile.packed_decoder()),
                Encoder::Pure(profile.encoder()),
            ),
        }
    }
}

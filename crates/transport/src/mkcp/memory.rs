//! On-demand host credit follows each real reliable payload until ACK/consume.
//! The fixed credit covers the duplex, scratch and finite retransmit/ACK peak.
#[cfg(feature = "quic")]
use super::Settings;
use bytes::Bytes;
use std::{io, sync::Arc};
use zero_platform_tokio::controlled_carrier::CarrierBufferBudget;
use zero_platform_tokio::controlled_carrier::CarrierBufferCredit;
#[derive(Clone)]
pub(super) struct Memory {
    pub book: CarrierBufferBudget,
    _working: Arc<CarrierBufferCredit>,
}
struct Payload {
    bytes: Vec<u8>,
    _credit: CarrierBufferCredit,
}
impl AsRef<[u8]> for Payload {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
impl Memory {
    #[cfg(feature = "quic")]
    pub fn prepare(settings: Settings, book: CarrierBufferBudget) -> io::Result<Self> {
        settings.validate()?;
        let duplex = settings.write_buffer_bytes.clamp(65536, 2 * 1024 * 1024) as usize;
        let send = settings.send_buffer();
        let receive = settings.receive_window() as usize;
        let checked = || -> Option<usize> {
            // Sender queue, receiver hash buckets, ACK lists, cloned flush
            // descriptors and encoded frames are all bounded by these windows.
            duplex
                .checked_mul(2)?
                .checked_add(65536 + settings.mss())?
                .checked_add(send.checked_mul(512 + 2 * settings.mtu as usize)?)?
                .checked_add(receive.checked_mul(1024)?)?
                .checked_add(65536)
        };
        let peak = checked().ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
        let credit = book.try_reserve(peak)?;
        Ok(Self {
            book,
            _working: Arc::new(credit),
        })
    }
    pub fn copy(&self, bytes: &[u8]) -> io::Result<Bytes> {
        Ok(self.copy_prepaid(bytes, self.book.try_reserve(bytes.len())?))
    }
    pub fn copy_prepaid(&self, bytes: &[u8], credit: CarrierBufferCredit) -> Bytes {
        Bytes::from_owner(Payload {
            bytes: bytes.to_vec(),
            _credit: credit,
        })
    }
}

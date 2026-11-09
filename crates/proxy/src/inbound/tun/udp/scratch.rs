//! This CPU receive allocation is separate from wire/read/codec storage.
use std::{
    io,
    ops::{Deref, DerefMut},
};
const BYTES: usize = 65_535; // the original actual dispatcher buffer length
pub(crate) struct UdpScratch {
    // Box deallocates before the following credit can refund/wake the Root.
    bytes: Box<[u8]>,
    #[cfg(feature = "raw-ip-runtime")]
    credit: Option<zero_tun::packet_memory::PacketMetadataCredit>,
}
impl UdpScratch {
    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) fn prepare(memory: &zero_tun::packet_memory::PacketMemoryView) -> io::Result<Self> {
        let credit = memory
            .reserve_driver_metadata(
                zero_tun::packet_memory::PacketMetadataLayout::array::<u8>(BYTES)
                    .map_err(io::Error::other)?,
            )
            .map_err(io::Error::other)?;
        // Exact requested Box array, no spare Vec capacity or old/new realloc.
        Ok(Self {
            bytes: zeroed(),
            credit: Some(credit),
        })
    }
    #[cfg(feature = "raw-ip-runtime")]
    pub(super) fn is_owned(&self) -> bool {
        self.credit.is_some()
    }
    #[cfg(any(test, not(feature = "raw-ip-runtime")))]
    pub(super) fn library() -> Self {
        Self {
            bytes: zeroed(),
            #[cfg(feature = "raw-ip-runtime")]
            credit: None,
        }
    }
}
fn zeroed() -> Box<[u8]> {
    let mut bytes = Box::<[u8]>::new_uninit_slice(BYTES);
    for byte in &mut bytes {
        byte.write(0);
    }
    unsafe { bytes.assume_init() }
}
impl Deref for UdpScratch {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.bytes
    }
}
impl DerefMut for UdpScratch {
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.bytes
    }
}
#[cfg(all(test, feature = "raw-ip-runtime"))]
mod tests;

//! Exact prepaid request workspace; no allocator callback or independent Root.
use super::*;
use crate::packet_memory::{PacketMetadataCredit, PacketMetadataLayout};
pub(in crate::macos_privilege::session) struct Request {
    bytes: Box<[u8]>,
    _credit: Option<PacketMetadataCredit>,
}
impl Request {
    pub(in crate::macos_privilege::session) fn layout(bytes: usize) -> PacketMetadataLayout {
        PacketMetadataLayout::new(1, bytes)
    }
    pub(super) fn from_prepaid(bytes: usize, credit: PacketMetadataCredit) -> io::Result<Self> {
        let paid = credit.layout();
        if bytes < CAPACITY || paid.allocations() < 1 || paid.bytes() < bytes {
            return Err(io::Error::other(
                "same helper request workspace was not prepaid",
            ));
        }
        Ok(Self {
            bytes: zeroed(bytes),
            _credit: Some(credit),
        })
    }
    #[cfg(test)]
    pub(super) fn library() -> Self {
        Self {
            bytes: zeroed(CAPACITY),
            _credit: None,
        }
    }
}
impl std::ops::Deref for Request {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.bytes
    }
}
impl std::ops::DerefMut for Request {
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.bytes
    }
}
fn zeroed(bytes: usize) -> Box<[u8]> {
    let mut value = Box::<[u8]>::new_uninit_slice(bytes);
    for byte in &mut value {
        byte.write(0);
    }
    unsafe { value.assume_init() }
}

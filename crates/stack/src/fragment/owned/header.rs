use super::*;
pub(super) const MAX_STORAGE: usize = u16::MAX as usize + 40;
#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) struct Header {
    pub length: usize,
    previous_next_header: Option<usize>,
    next_header: u8,
}
impl Header {
    pub(super) fn parse(packet: &[u8], parsed: &ParsedIpFragment<'_>) -> Option<Self> {
        let payload = (parsed.payload.as_ptr() as usize).checked_sub(packet.as_ptr() as usize)?;
        match packet[0] >> 4 {
            4 => Some(Self {
                length: payload,
                previous_next_header: None,
                next_header: parsed.key.next_header,
            }),
            6 => {
                let mut next = packet[6];
                let mut previous = 6;
                let mut offset: usize = 40;
                for _ in 0..8 {
                    if next == 44 {
                        return (offset + 8 == payload).then_some(Self {
                            length: offset,
                            previous_next_header: Some(previous),
                            next_header: parsed.key.next_header,
                        });
                    }
                    let following = *packet.get(offset)?;
                    let size = usize::from(*packet.get(offset + 1)?);
                    let length = match next {
                        0 | 43 | 60 => (size + 1) * 8,
                        51 => (size + 2) * 4,
                        _ => return None,
                    };
                    previous = offset;
                    next = following;
                    offset = offset.checked_add(length)?;
                }
                None
            }
            _ => None,
        }
    }
    pub(super) fn copy_first(&self, target: &mut [u8], packet: &[u8]) {
        target[..self.length].copy_from_slice(&packet[..self.length]);
    }
    pub(super) fn matches_first(&self, storage: &[u8], packet: &[u8]) -> bool {
        storage[..self.length] == packet[..self.length]
    }
    pub(super) fn finish(&self, packet: &mut [u8], payload: usize) -> Option<usize> {
        let total = self.length.checked_add(payload)?;
        if total > packet.len() {
            return None;
        }
        match self.previous_next_header {
            None => {
                packet[2..4].copy_from_slice(&u16::try_from(total).ok()?.to_be_bytes());
                let flags = u16::from_be_bytes([packet[6], packet[7]]) & 0x4000;
                packet[6..8].copy_from_slice(&flags.to_be_bytes());
                packet[10..12].fill(0);
                let checksum = crate::packet::checksum(&packet[..self.length]);
                packet[10..12].copy_from_slice(&checksum.to_be_bytes());
            }
            Some(previous) => {
                packet[previous] = self.next_header;
                packet[4..6]
                    .copy_from_slice(&u16::try_from(total.checked_sub(40)?).ok()?.to_be_bytes());
            }
        }
        Some(total)
    }
}

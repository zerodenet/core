//! Bounded traffic classification matching the pinned Vision filter window.
// The window is shared by both directions; only a recognized ServerHello can
// authorize raw TLS record forwarding, and CCM-8 remains excluded.
pub(super) struct TlsFilter {
    pub(super) remaining_packets: u8,
    pub(super) is_tls: bool,
    pub(super) tls12_or_above: bool,
    pub(super) direct: bool,
    remaining_server_hello: usize,
    cipher: u16,
}
impl Default for TlsFilter {
    fn default() -> Self {
        Self {
            remaining_packets: 8,
            is_tls: false,
            tls12_or_above: false,
            direct: false,
            remaining_server_hello: 0,
            cipher: 0,
        }
    }
}
impl TlsFilter {
    pub(super) fn observe(&mut self, content: &[u8]) {
        if self.remaining_packets == 0 || content.is_empty() {
            return;
        }
        self.remaining_packets -= 1;
        if content.len() >= 6 {
            if content[..3] == [0x16, 0x03, 0x03] && content[5] == 2 {
                self.remaining_server_hello =
                    usize::from(u16::from_be_bytes([content[3], content[4]])) + 5;
                self.tls12_or_above = true;
                self.is_tls = true;
                if content.len() >= 79 && self.remaining_server_hello >= 79 {
                    let offset = 44 + usize::from(content[43]);
                    if let Some(cipher) = content.get(offset..offset + 2) {
                        self.cipher = u16::from_be_bytes([cipher[0], cipher[1]]);
                    }
                }
            } else if content[..2] == [0x16, 0x03] && content[5] == 1 {
                self.is_tls = true;
            }
        }
        if self.remaining_server_hello == 0 {
            return;
        }
        let end = self.remaining_server_hello.min(content.len());
        self.remaining_server_hello = self.remaining_server_hello.saturating_sub(content.len());
        if content[..end]
            .windows(6)
            .any(|window| window == [0, 0x2b, 0, 2, 3, 4])
        {
            self.direct = matches!(self.cipher, 0x1301..=0x1304);
            self.remaining_packets = 0;
        } else if self.remaining_server_hello == 0 {
            self.remaining_packets = 0;
        }
    }
}

pub(super) fn complete_application_records(mut content: &[u8]) -> bool {
    if content.len() < 6 {
        return false;
    }
    while !content.is_empty() {
        if content.len() < 5 || content[..3] != [0x17, 3, 3] {
            return false;
        }
        let size = usize::from(u16::from_be_bytes([content[3], content[4]]));
        if size == 0 || content.len() < 5 + size {
            return false;
        }
        content = &content[5 + size..];
    }
    true
}

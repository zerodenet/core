use super::*;
pub(super) struct Header {
    kind: Kind,
}
enum Kind {
    Fixed(Vec<u8>),
    Dns(Vec<u8>),
    Dtls {
        epoch: u16,
        sequence: u32,
        length: u16,
    },
    Srtp(u16),
    Wechat(u32),
    Custom {
        send: Vec<Item>,
        receive: Vec<Item>,
        size: usize,
    },
}
impl Header {
    pub(super) fn new(mask: &Mask, server: bool) -> io::Result<Self> {
        let kind = match mask {
            Mask::Wireguard => Kind::Fixed(vec![4, 0, 0, 0]),
            Mask::Utp => {
                let mut header = rand::random::<u16>().to_be_bytes().to_vec();
                header.extend([1, 0]);
                Kind::Fixed(header)
            }
            Mask::Srtp => Kind::Srtp(rand::random()),
            Mask::Wechat => Kind::Wechat(u32::from(rand::random::<u16>())),
            Mask::Dtls => Kind::Dtls {
                epoch: rand::random(),
                sequence: 0,
                length: 17,
            },
            Mask::Dns { domain } => Kind::Dns(dns(domain)?),
            Mask::Custom {
                client,
                server: peer,
            } => {
                for item in client.iter().chain(peer) {
                    if let Item::Random {
                        minimum, maximum, ..
                    } = item
                    {
                        if minimum > maximum {
                            return Err(invalid("invalid FinalMask random byte range"));
                        }
                    }
                }
                let a = client
                    .iter()
                    .map(Item::length)
                    .try_fold(0usize, usize::checked_add)
                    .ok_or_else(|| invalid("FinalMask custom header is too large"))?;
                let b = peer
                    .iter()
                    .map(Item::length)
                    .try_fold(0usize, usize::checked_add)
                    .ok_or_else(|| invalid("FinalMask custom header is too large"))?;
                if a != b || a > 4096 {
                    return Err(invalid(
                        "FinalMask UDP custom headers require equal bounded sizes",
                    ));
                }
                let (send, receive) = if server {
                    (peer, client)
                } else {
                    (client, peer)
                };
                Kind::Custom {
                    send: send.clone(),
                    receive: receive.clone(),
                    size: a,
                }
            }
            _ => return Err(invalid("not a header mask")),
        };
        Ok(Self { kind })
    }
    pub(super) fn encode(&mut self, payload: &[u8]) -> Vec<u8> {
        let mut out = match &mut self.kind {
            Kind::Fixed(bytes) => bytes.clone(),
            Kind::Dns(bytes) => {
                let mut out = bytes.clone();
                out[..2].copy_from_slice(&rand::random::<u16>().to_be_bytes());
                out
            }
            Kind::Srtp(number) => {
                *number = number.wrapping_add(1);
                let mut out = vec![0xb5, 0xe8];
                out.extend(number.to_be_bytes());
                out
            }
            Kind::Wechat(sequence) => {
                *sequence = sequence.wrapping_add(1);
                let mut out = vec![0xa1, 0x08];
                out.extend(sequence.to_be_bytes());
                out.extend([0, 0x10, 0x11, 0x18, 0x30, 0x22, 0x30]);
                out
            }
            Kind::Dtls {
                epoch,
                sequence,
                length,
            } => {
                let mut out = vec![23, 254, 253];
                out.extend(epoch.to_be_bytes());
                out.extend([0, 0]);
                out.extend(sequence.to_be_bytes());
                out.extend(length.to_be_bytes());
                *sequence = sequence.wrapping_add(1);
                *length += 17;
                if *length > 100 {
                    *length -= 50;
                }
                out
            }
            Kind::Custom { send, .. } => {
                let mut out = Vec::new();
                for item in send {
                    item.append(&mut out);
                }
                out
            }
        };
        out.extend(payload);
        out
    }
    pub(super) fn decode(&self, packet: &[u8]) -> io::Result<Vec<u8>> {
        let size = match &self.kind {
            Kind::Fixed(bytes) | Kind::Dns(bytes) => bytes.len(),
            Kind::Srtp(_) => 4,
            Kind::Wechat(_) | Kind::Dtls { .. } => 13,
            Kind::Custom { size, .. } => *size,
        };
        if packet.len() < size {
            return Err(invalid("short FinalMask header"));
        }
        if let Kind::Custom { receive, .. } = &self.kind {
            let mut offset = 0;
            for item in receive {
                if let Item::Bytes(bytes) = item {
                    if &packet[offset..offset + bytes.len()] != bytes {
                        return Err(invalid("FinalMask custom header mismatch"));
                    }
                }
                offset += item.length();
            }
        }
        Ok(packet[size..].to_vec())
    }
}
fn dns(domain: &str) -> io::Result<Vec<u8>> {
    let domain = if domain.is_empty() {
        "www.baidu.com"
    } else {
        domain
    };
    if domain.len() > 4096 {
        return Err(invalid("DNS mask domain is too large"));
    }
    let mut out = vec![0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0];
    let mut label = Vec::new();
    let mut escaped = false;
    for byte in domain.bytes().chain(std::iter::once(b'.')) {
        if escaped {
            label.push(byte);
            escaped = false;
        } else if byte == b'\\' {
            escaped = true;
        } else if byte == b'.' {
            if label.len() > 63 || out.len() + 1 + label.len() > 267 {
                return Err(invalid("DNS mask domain exceeds wire buffer"));
            }
            out.push(label.len() as u8);
            out.append(&mut label);
        } else {
            label.push(byte);
        }
    }
    // The pinned header encoder emits even empty labels and only flushes at
    // unescaped dots. This header is a fixed-size camouflage, not a DNS resolver.
    out.extend([0, 0, 1, 0, 1]);
    Ok(out)
}

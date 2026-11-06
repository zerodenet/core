//! Bounded line streaming; /proc/net is never read_to_string or copied whole.
use super::*;
struct Lines {
    bytes: Vec<u8>,
    offset: usize,
    length: usize,
    consumed: usize,
    limit: usize,
}
impl Lines {
    fn new(limit: usize) -> Self {
        Self {
            bytes: vec![0; limit.min(8192)],
            offset: 0,
            length: 0,
            consumed: 0,
            limit,
        }
    }
    fn next(&mut self, originals: &mut Originals) -> Result<Option<String>, Arc<ChannelError>> {
        let mut line = Vec::new();
        loop {
            if self.offset == self.length {
                let remaining = self.limit.saturating_sub(self.consumed);
                if remaining == 0 {
                    // The one-byte overflow probe is bounded by the validated
                    // configured limit+1, not an uncharged read_to_end.
                    let mut probe = [0];
                    if originals.read(FILE, &mut probe).map_err(io_failure)? != 0 {
                        return Err(capacity());
                    }
                    return if line.is_empty() {
                        Ok(None)
                    } else {
                        String::from_utf8(line).map(Some).map_err(|_| invalid())
                    };
                }
                let size = remaining.min(self.bytes.len());
                self.length = originals
                    .read(FILE, &mut self.bytes[..size])
                    .map_err(io_failure)?;
                self.offset = 0;
                self.consumed += self.length;
                if self.length == 0 {
                    return if line.is_empty() {
                        Ok(None)
                    } else {
                        String::from_utf8(line).map(Some).map_err(|_| invalid())
                    };
                }
            }
            let bytes = &self.bytes[self.offset..self.length];
            let end = bytes.iter().position(|byte| *byte == b'\n');
            let size = end.map_or(bytes.len(), |end| end + 1);
            line.extend_from_slice(&bytes[..size]);
            self.offset += size;
            if end.is_some() {
                return String::from_utf8(line).map(Some).map_err(|_| invalid());
            }
        }
    }
}
pub(super) fn inode(
    originals: &mut Originals,
    source: SocketAddr,
    transport: LocalProcessTransport,
    limit: usize,
) -> Result<Option<u64>, Arc<ChannelError>> {
    let (table, ip, wildcard) = match source.ip() {
        std::net::IpAddr::V4(ip) => {
            let octets = ip.octets();
            (
                if transport == LocalProcessTransport::Tcp {
                    "tcp"
                } else {
                    "udp"
                },
                format!(
                    "{:02X}{:02X}{:02X}{:02X}",
                    octets[3], octets[2], octets[1], octets[0]
                ),
                "00000000",
            )
        }
        std::net::IpAddr::V6(ip) => {
            let mut encoded = String::with_capacity(32);
            for chunk in ip.octets().as_chunks::<4>().0 {
                for byte in chunk.iter().rev() {
                    use std::fmt::Write;
                    write!(&mut encoded, "{byte:02X}").unwrap();
                }
            }
            (
                if transport == LocalProcessTransport::Tcp {
                    "tcp6"
                } else {
                    "udp6"
                },
                encoded,
                "00000000000000000000000000000000",
            )
        }
    };
    if originals
        .open(
            FILE,
            &std::path::PathBuf::from(format!("/proc/net/{table}")),
            false,
        )
        .is_err()
    {
        return Ok(None);
    }
    let result = (|| {
        let mut lines = Lines::new(limit);
        let mut header = true;
        let port = format!("{:04X}", source.port());
        while let Some(line) = lines.next(originals)? {
            if header {
                header = false;
                continue;
            }
            let mut fields = line.split_whitespace();
            let Some(local) = fields.nth(1) else {
                continue;
            };
            let Some((local_ip, local_port)) = local.split_once(':') else {
                continue;
            };
            if (local_ip != ip && local_ip != wildcard) || local_port != port {
                continue;
            }
            return Ok(fields.nth(7).and_then(|inode| inode.parse().ok()));
        }
        Ok(None)
    })();
    if let Err(source) = &result {
        originals.retain_operation_failure(source.clone());
    }
    originals.close(FILE)?;
    result
}
pub(super) fn name(
    originals: &mut Originals,
    pid: u32,
    limit: usize,
) -> Result<Option<String>, Arc<ChannelError>> {
    if originals
        .open(
            FILE,
            &std::path::PathBuf::from(format!("/proc/{pid}/comm")),
            false,
        )
        .is_err()
    {
        return Ok(None);
    }
    let result = (|| {
        let mut lines = Lines::new(limit);
        let mut name = String::new();
        while let Some(line) = lines.next(originals)? {
            name.push_str(&line);
        }
        Ok(Some(name.trim().to_owned()))
    })();
    if let Err(source) = &result {
        originals.retain_operation_failure(source.clone());
    }
    originals.close(FILE)?;
    result
}

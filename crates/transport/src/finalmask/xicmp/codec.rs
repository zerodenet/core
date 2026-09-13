//! Echo wire messages and the bounded XICMP response sequence window.
use std::{
    collections::HashMap,
    io,
    net::{IpAddr, SocketAddr},
};
pub struct Echo<'a> {
    pub id: u16,
    pub sequence: u16,
    pub payload: &'a [u8],
}
pub fn encode(
    ipv6: bool,
    reply: bool,
    id: u16,
    sequence: u16,
    payload: &[u8],
) -> io::Result<Vec<u8>> {
    if payload.len() + 8 > 65535 {
        return Err(invalid());
    }
    let mut wire = vec![
        match (ipv6, reply) {
            (false, false) => 8,
            (false, true) => 0,
            (true, false) => 128,
            (true, true) => 129,
        },
        0,
        0,
        0,
    ];
    wire.extend(id.to_be_bytes());
    wire.extend(sequence.to_be_bytes());
    wire.extend(payload);
    if !ipv6 {
        let checksum = checksum(&wire);
        wire[2..4].copy_from_slice(&checksum.to_be_bytes());
    }
    Ok(wire)
}
pub fn parse(wire: &[u8], ipv6: bool, reply: bool) -> io::Result<Echo<'_>> {
    let kind = match (ipv6, reply) {
        (false, false) => 8,
        (false, true) => 0,
        (true, false) => 128,
        (true, true) => 129,
    };
    if wire.len() < 8 || wire[0] != kind || wire[1] != 0 || (!ipv6 && checksum(wire) != 0) {
        return Err(invalid());
    }
    Ok(Echo {
        id: u16::from_be_bytes([wire[4], wire[5]]),
        sequence: u16::from_be_bytes([wire[6], wire[7]]),
        payload: &wire[8..],
    })
}
fn checksum(bytes: &[u8]) -> u16 {
    let mut sum = 0u32;
    for chunk in bytes.chunks(2) {
        sum += u32::from(u16::from_be_bytes([chunk[0], *chunk.get(1).unwrap_or(&0)]));
    }
    while sum >> 16 != 0 {
        sum = (sum & 65535) + (sum >> 16);
    }
    !(sum as u16)
}
struct Status {
    first: Option<u8>,
    peer: SocketAddr,
}
pub struct Client {
    id: u16,
    ipv6: bool,
    next: u16,
    requests: HashMap<u16, Status>,
}
impl Client {
    pub fn new(id: u16, ipv6: bool) -> Self {
        Self {
            id,
            ipv6,
            next: 1,
            requests: HashMap::new(),
        }
    }
    pub fn query(&mut self, payload: &[u8], peer: SocketAddr) -> io::Result<Vec<u8>> {
        let sequence = self.next;
        let bytes = encode(self.ipv6, false, self.id, sequence, payload)?;
        self.requests.insert(
            sequence,
            Status {
                first: payload.first().copied(),
                peer,
            },
        );
        self.requests.remove(&sequence.wrapping_sub(1000));
        self.next = self.next.wrapping_add(1);
        if self.next == 0 {
            self.requests.remove(&0u16.wrapping_sub(1000));
            self.next = 1;
        }
        Ok(bytes)
    }
    pub fn response(
        &mut self,
        wire: &[u8],
        source: IpAddr,
    ) -> io::Result<Option<(Vec<u8>, SocketAddr)>> {
        let echo = parse(wire, self.ipv6, true)?;
        if echo.id != self.id {
            return Ok(None);
        }
        let Some(status) = self.requests.get(&echo.sequence) else {
            return Ok(None);
        };
        if status.peer.ip() != source {
            return Ok(None);
        }
        let payload = if let Some(first) = status.first {
            if echo.payload.len() <= 1 || echo.payload[0] == first {
                return Ok(None);
            }
            &echo.payload[1..]
        } else {
            echo.payload
        };
        if payload.is_empty() {
            return Ok(None);
        }
        let peer = status.peer;
        self.requests.remove(&echo.sequence);
        Ok(Some((payload.to_vec(), peer)))
    }
}
pub fn reply(
    ipv6: bool,
    id: u16,
    sequence: u16,
    first: Option<u8>,
    payload: &[u8],
) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    if let Some(first) = first {
        let mut byte = rand::random::<u8>();
        while byte == first {
            byte = rand::random();
        }
        bytes.push(byte);
    }
    bytes.extend(payload);
    encode(ipv6, true, id, sequence, &bytes)
}
fn invalid() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "invalid XICMP echo message")
}

use bytes::Bytes;
use std::io;

pub(super) const ACK: u8 = 0;
pub(super) const DATA: u8 = 1;
pub(super) const TERMINATE: u8 = 2;
pub(super) const PING: u8 = 3;
pub(super) const CLOSE: u8 = 1;
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Segment {
    pub conv: u16,
    pub option: u8,
    pub body: Body,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Body {
    Data {
        timestamp: u32,
        number: u32,
        sending_next: u32,
        payload: Bytes,
    },
    Ack {
        window: u32,
        next: u32,
        timestamp: u32,
        numbers: Vec<u32>,
    },
    Command {
        command: u8,
        sending_next: u32,
        receiving_next: u32,
        rto: u32,
    },
}
impl Segment {
    pub(super) fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(32);
        bytes.extend_from_slice(&self.conv.to_be_bytes());
        bytes.push(match &self.body {
            Body::Data { .. } => DATA,
            Body::Ack { .. } => ACK,
            Body::Command { command, .. } => *command,
        });
        bytes.push(self.option);
        match &self.body {
            Body::Data {
                timestamp,
                number,
                sending_next,
                payload,
            } => {
                for value in [timestamp, number, sending_next] {
                    bytes.extend_from_slice(&value.to_be_bytes());
                }
                bytes.extend_from_slice(&(payload.len() as u16).to_be_bytes());
                bytes.extend_from_slice(payload);
            }
            Body::Ack {
                window,
                next,
                timestamp,
                numbers,
            } => {
                for value in [window, next, timestamp] {
                    bytes.extend_from_slice(&value.to_be_bytes());
                }
                bytes.push(numbers.len() as u8);
                for number in numbers {
                    bytes.extend_from_slice(&number.to_be_bytes());
                }
            }
            Body::Command {
                sending_next,
                receiving_next,
                rto,
                ..
            } => {
                for value in [sending_next, receiving_next, rto] {
                    bytes.extend_from_slice(&value.to_be_bytes());
                }
            }
        }
        bytes
    }
    pub(super) fn decode(bytes: &mut &[u8]) -> io::Result<Self> {
        let header = take(bytes, 4)?;
        let conv = u16::from_be_bytes([header[0], header[1]]);
        let option = header[3];
        let body = match header[2] {
            DATA => {
                let timestamp = number(bytes)?;
                let number = number(bytes)?;
                let sending_next = self::number(bytes)?;
                let length = take(bytes, 2)?;
                let length = u16::from_be_bytes([length[0], length[1]]) as usize;
                if length == 0 {
                    return Err(invalid());
                }
                Body::Data {
                    timestamp,
                    number,
                    sending_next,
                    payload: Bytes::copy_from_slice(take(bytes, length)?),
                }
            }
            ACK => {
                let window = number(bytes)?;
                let next = number(bytes)?;
                let timestamp = number(bytes)?;
                let count = take(bytes, 1)?[0];
                let mut numbers = Vec::with_capacity(count as usize);
                for _ in 0..count {
                    numbers.push(number(bytes)?);
                }
                Body::Ack {
                    window,
                    next,
                    timestamp,
                    numbers,
                }
            }
            command => Body::Command {
                command,
                sending_next: number(bytes)?,
                receiving_next: number(bytes)?,
                rto: number(bytes)?,
            },
        };
        Ok(Self { conv, option, body })
    }
}
fn number(bytes: &mut &[u8]) -> io::Result<u32> {
    Ok(u32::from_be_bytes(take(bytes, 4)?.try_into().unwrap()))
}
fn take<'a>(bytes: &mut &'a [u8], n: usize) -> io::Result<&'a [u8]> {
    if bytes.len() < n {
        return Err(invalid());
    }
    let (head, tail) = bytes.split_at(n);
    *bytes = tail;
    Ok(head)
}
fn invalid() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "truncated or empty mKCP segment",
    )
}

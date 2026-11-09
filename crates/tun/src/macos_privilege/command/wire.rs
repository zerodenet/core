//! Private --owned command framing on the original control, never a close proof.
use std::io::{self, Write};
use std::os::unix::net::UnixStream;
pub(in crate::macos_privilege) const HEADER: usize = 13;
pub(in crate::macos_privilege) const CHUNK: usize = 4096;
pub(in crate::macos_privilege) const FINISH: usize = 60;
#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::macos_privilege) enum Kind {
    Stdout = 1,
    Stderr = 2,
    Finish = 3,
}
pub(in crate::macos_privilege) struct Frame {
    pub kind: Kind,
    pub length: usize,
}
impl Frame {
    pub(in crate::macos_privilege) fn decode(bytes: &[u8; HEADER], nonce: u64) -> io::Result<Self> {
        if u64::from_be_bytes(bytes[1..9].try_into().unwrap()) != nonce {
            return Err(io::Error::other(
                "command segment belongs to another actual request",
            ));
        }
        let kind = match bytes[0] {
            1 => Kind::Stdout,
            2 => Kind::Stderr,
            3 => Kind::Finish,
            _ => return Err(io::Error::other("invalid owned command segment")),
        };
        let length = u32::from_be_bytes(bytes[9..].try_into().unwrap()) as usize;
        if match kind {
            Kind::Finish => length != FINISH,
            _ => length == 0 || length > CHUNK,
        } {
            return Err(io::Error::other(
                "owned command segment exceeds its fixed framing",
            ));
        }
        Ok(Self { kind, length })
    }
}
pub(in crate::macos_privilege) fn write(
    stream: &mut UnixStream,
    nonce: u64,
    kind: Kind,
    bytes: &[u8],
) -> io::Result<()> {
    let length = u32::try_from(bytes.len()).map_err(io::Error::other)?;
    let mut header = [0; HEADER];
    header[0] = kind as u8;
    header[1..9].copy_from_slice(&nonce.to_be_bytes());
    header[9..].copy_from_slice(&length.to_be_bytes());
    Frame::decode(&header, nonce)?;
    stream.write_all(&header)?;
    stream.write_all(bytes)
}
/// Original helper-side outcomes only. These integers cannot stand in for the
/// parent's own registered tool exit, helper exit, transferred FD or Host ACK.
pub(in crate::macos_privilege) struct Finished {
    pub status: i32,
    pub child_pid: i32,
    pub operation_errno: i32,
    pub wait_errno: i32,
    pub cancel_errno: i32,
    pub close_errnos: [i32; 10],
}
impl Finished {
    pub(in crate::macos_privilege) fn encode(&self) -> [u8; FINISH] {
        let words = [
            self.status,
            self.child_pid,
            self.operation_errno,
            self.wait_errno,
            self.cancel_errno,
        ];
        let mut bytes = [0; FINISH];
        for (slot, word) in bytes
            .chunks_exact_mut(4)
            .zip(words.into_iter().chain(self.close_errnos))
        {
            slot.copy_from_slice(&word.to_be_bytes());
        }
        bytes
    }
    pub(in crate::macos_privilege) fn decode(bytes: &[u8; FINISH]) -> Self {
        let words: [i32; 15] = std::array::from_fn(|index| {
            i32::from_be_bytes(bytes[index * 4..index * 4 + 4].try_into().unwrap())
        });
        Self {
            status: words[0],
            child_pid: words[1],
            operation_errno: words[2],
            wait_errno: words[3],
            cancel_errno: words[4],
            close_errnos: std::array::from_fn(|index| words[index + 5]),
        }
    }
}

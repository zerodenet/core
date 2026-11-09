//! Every retained capacity is charged before allocation and dropped before credit.
use super::*;
use tokio::io::{AsyncBufRead, AsyncBufReadExt};
use zero_runtime::configuration::{ControlBudgetPermit, ControlCharge};
pub(super) struct Frame {
    pub(super) bytes: Vec<u8>,
    _credit: ControlBudgetPermit,
}
fn alloc(error: std::collections::TryReserveError) -> io::Error {
    io::Error::other(error)
}
impl Frame {
    pub(super) fn request(
        value: &impl serde::Serialize,
        book: &Arc<ControlBudget>,
    ) -> io::Result<Self> {
        let size = crate::ipc::owner::serialized_len(value)?
            .checked_add(1)
            .ok_or_else(|| io::Error::other("client frame length overflow"))?;
        if size > book.limits().messages.ipc_frame_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "control client request exceeds frame limit",
            ));
        }
        let credit = book.reserve(ControlCharge::RequestBytes(size))?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(size).map_err(alloc)?;
        serde_json::to_writer(
            Bounded {
                bytes: &mut bytes,
                end: size - 1,
            },
            value,
        )
        .map_err(io::Error::other)?;
        bytes.push(b'\n');
        Ok(Self {
            bytes,
            _credit: credit,
        })
    }
    // Only the concrete TUN projection calls this after its formatter
    // workspace is prepaid. This preserves Value's duplicate-last semantics
    // without constructing any container Value or sharing mutable snapshots.
    pub(super) fn project(
        value: &impl serde::Serialize,
        book: &Arc<ControlBudget>,
    ) -> io::Result<Self> {
        let size = crate::ipc::owner::serialized_len(value)?;
        let credit = book.reserve(ControlCharge::ResponseBytes(size))?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(size).map_err(alloc)?;
        serde_json::to_writer(
            Bounded {
                bytes: &mut bytes,
                end: size,
            },
            value,
        )
        .map_err(io::Error::other)?;
        Ok(Self {
            bytes,
            _credit: credit,
        })
    }
    pub(super) async fn read(
        reader: &mut (impl AsyncBufRead + Unpin),
        book: &Arc<ControlBudget>,
    ) -> io::Result<Option<Self>> {
        let mut frame = Self {
            bytes: Vec::new(),
            _credit: book.reserve(ControlCharge::ResponseBytes(0))?,
        };
        loop {
            let input = reader.fill_buf().await?;
            if input.is_empty() {
                return Ok((!frame.bytes.is_empty()).then_some(frame));
            }
            let size = input
                .iter()
                .position(|b| *b == b'\n')
                .map_or(input.len(), |n| n + 1);
            let end = frame
                .bytes
                .len()
                .checked_add(size)
                .ok_or_else(|| io::Error::other("client frame length overflow"))?;
            if end > book.limits().messages.ipc_frame_bytes {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "control client response exceeds frame limit",
                ));
            }
            if end > frame.bytes.capacity() {
                let growth = end - frame.bytes.capacity();
                frame._credit.grow(growth)?;
                frame
                    .bytes
                    .try_reserve_exact(end - frame.bytes.len())
                    .map_err(alloc)?;
            }
            frame.bytes.extend_from_slice(&input[..size]);
            reader.consume(size);
            if frame.bytes.last() == Some(&b'\n') {
                return Ok(Some(frame));
            }
        }
    }
}

struct Bounded<'a> {
    bytes: &'a mut Vec<u8>,
    end: usize,
}
impl io::Write for Bounded<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self
            .bytes
            .len()
            .checked_add(bytes.len())
            .is_none_or(|end| end > self.end)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "control client request changed after measurement",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

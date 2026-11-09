//! Concrete TUN DTO only, after paying its actual canonical frame/workspace.
use super::*;
pub(crate) struct Typed<T> {
    value: T,
    _credit: ControlBudgetPermit,
}
impl<T> std::ops::Deref for Typed<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}
impl Reply {
    pub(crate) fn tun_status(&self) -> io::Result<Typed<zero_api::TunStatusSnapshot>> {
        let workspace = self.book.reserve(ControlCharge::ResponseBytes(
            self.layout.workspace(self.frame.bytes.len())?,
        ))?;
        let header = self.header()?;
        let result = header.result.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "unexpected response to TUN status query",
            )
        })?;
        let raw = format::tun_value(result)?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "unexpected response to TUN status query",
            )
        })?;
        let canonical = frames::Frame::project(&format::Wire(raw), &self.book)?;
        let raw =
            serde_json::from_slice::<&RawValue>(&canonical.bytes).map_err(io::Error::other)?;
        let rows = field::tun_array_rows(raw).map_err(io::Error::other)?;
        let bytes = rows
            .checked_mul(std::mem::size_of::<String>())
            .and_then(|n| n.checked_add(raw.get().len().checked_mul(4)?))
            .and_then(|n| n.checked_add(self.layout.scratch().ok()?))
            .ok_or_else(capacity)?;
        let credit = self.book.reserve(ControlCharge::ResponseBytes(bytes))?;
        let value = match serde_json::from_str::<zero_api::TunStatusSnapshot>(raw.get()) {
            Ok(value) => value,
            Err(error) => return Err(workspace_error(error, credit)),
        };
        drop(canonical);
        drop(header);
        drop(workspace);
        Ok(Typed {
            value,
            _credit: credit,
        })
    }
}

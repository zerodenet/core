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
        if semantic::has_marker(result).map_err(io::Error::other)? {
            let mut original = semantic::value(result, &self.book)?;
            let extra = zero_runtime::configuration::control_json_response_storage_bound(
                result.get().as_bytes(),
                &self.book,
                zero_runtime::configuration::ControlJsonStorage::Value,
            )?;
            original._credit.grow(
                extra
                    .checked_add(result.get().len().checked_mul(4).ok_or_else(capacity)?)
                    .and_then(|n| n.checked_add(4096))
                    .ok_or_else(capacity)?,
            )?;
            let Guard { _credit, value } = original;
            let decoded = match serde_json::from_value::<zero_api::QueryResponse>(value) {
                Ok(decoded) => decoded,
                Err(error) => return Err(workspace_error(error, _credit)),
            };
            let zero_api::QueryResponse::TunStatus(value) = decoded else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unexpected response to TUN status query",
                ));
            };
            drop(header);
            drop(workspace);
            return Ok(Typed { value, _credit });
        }
        let raw = format::tun_value(result)?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "unexpected response to TUN status query",
            )
        })?;
        let wire = format::Wire::new(raw, &self.book)?;
        let canonical = frames::Frame::project(&wire, &self.book)?;
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
